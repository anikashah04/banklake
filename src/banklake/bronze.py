"""Bronze layer: raw CSV/JSON -> Parquet with ingestion metadata. Full + incremental loads."""
import hashlib
import json
import shutil
import time

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from . import meta, vault
from .common import RAW, layer_path
from .schemas import ENTITIES, SOURCE_SYSTEM


def _check_schema(path, spec):
    """Fail fast with a clear message if a source file does not have the expected columns."""
    with open(path) as f:
        first = f.readline().strip()
    cols = [c.strip() for c in first.split(",")] if spec["fmt"] == "csv" else list(json.loads(first).keys())
    missing = [c for c in spec["columns"] if c not in cols]
    if missing:
        raise ValueError(f"Schema mismatch in {path.name}: missing columns {missing}")


def _sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _audit(ctx, batch_id, entity, fname, strategy, fhash, read, loaded, rejected, status, message=""):
    meta.execute(
        "INSERT INTO ingestion_audit_log(run_id,batch_id,entity,source_file,source_system,load_type,file_hash,"
        "records_read,records_loaded,records_rejected,status,message,ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (ctx.run_id, batch_id, entity, fname, SOURCE_SYSTEM[entity], strategy.upper(), fhash,
         read, loaded, rejected, status, message, meta.now()))


def run(spark, meta_row, ctx):
    """Ingest the raw files of one entity.

    Per file: schema check -> duplicate-content check -> read as text -> split good rows from corrupt
    rows -> write Parquet -> record counts in ingested_files and ingestion_audit_log.
    A file with a wrong schema is rejected (copied to quarantine/rejected_files) while the valid files
    are still loaded; the pipeline then fails so the problem is visible and downstream stages are blocked.
    """
    entity = meta_row["source_table"].split("/")[-1]
    spec = ENTITIES[entity]
    strategy = "full" if ctx.full_refresh else meta_row["load_strategy"]
    folder = RAW / entity
    files = sorted(p for p in folder.glob("*") if p.suffix in (".csv", ".json"))
    if not files:
        raise FileNotFoundError(f"No source files found in {folder}")
    batch_id = int(time.time() * 1000)
    known = {r["file_name"]: r["file_hash"] for r in
             meta.query("SELECT file_name, file_hash FROM ingested_files WHERE entity=?", (entity,))}
    seen_hashes = {h for h in known.values() if h} if strategy != "full" else set()

    todo, rejected_files = [], []
    for f in files:
        if strategy != "full" and f.name in known:
            continue  # already ingested: re-runs never load a file twice
        fhash = _sha256(f)
        if fhash in seen_hashes:
            _audit(ctx, batch_id, entity, f.name, strategy, fhash, 0, 0, 0, "SKIPPED",
                   "duplicate file: identical content was already ingested")
            continue
        try:
            _check_schema(f, spec)
        except (ValueError, json.JSONDecodeError) as e:
            dest = layer_path("quarantine", "rejected_files") / entity
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, dest / f.name)
            _audit(ctx, batch_id, entity, f.name, strategy, fhash, 0, 0, 0, "FAILED", str(e))
            rejected_files.append(str(e))
            continue
        seen_hashes.add(fhash)
        todo.append((f, fhash))

    n_in = n_bad = 0
    if todo:
        schema = StructType([StructField(c, StringType(), True) for c in spec["columns"]]
                            + [StructField("_corrupt_record", StringType(), True)])
        paths = [str(f) for f, _ in todo]
        rd = spark.read.schema(schema).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record")
        df = rd.option("header", True).csv(paths) if spec["fmt"] == "csv" else rd.json(paths)
        df = df.withColumn("_source_file", F.regexp_extract(F.input_file_name(), r"[^/]+$", 0)).cache()

        bad = df.filter(F.col("_corrupt_record").isNotNull())
        good = (df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
                .withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_ingest_ts", F.current_timestamp())
                .withColumn("_load_type", F.lit(strategy)))
        # Bronze is the first masked layer: PII becomes masked value + token, real values go to the vault.
        good = vault.protect(good, entity, actor="bronze", action="MASK_AT_BRONZE")
        per_file = {r["_source_file"]: r["count"] for r in df.groupBy("_source_file").count().collect()}
        bad_per_file = {r["_source_file"]: r["count"] for r in bad.groupBy("_source_file").count().collect()}
        n_in = sum(per_file.values())
        n_bad = bad.count()

        mode = "overwrite" if strategy == "full" else "append"
        good.write.mode(mode).parquet(str(layer_path("bronze", entity)))
        bad.write.mode(mode).parquet(str(layer_path("quarantine", f"bronze_{entity}")))

        if strategy == "full":
            meta.execute("DELETE FROM ingested_files WHERE entity=?", (entity,))
        for f, fhash in todo:
            read = per_file.get(f.name, 0)
            rej = bad_per_file.get(f.name, 0)
            meta.execute("INSERT OR REPLACE INTO ingested_files(entity,file_name,batch_id,rows_read,rows_loaded,ts,file_hash) "
                         "VALUES(?,?,?,?,?,?,?)", (entity, f.name, batch_id, read, read - rej, meta.now(), fhash))
            _audit(ctx, batch_id, entity, f.name, strategy, fhash, read, read - rej, rej, "SUCCESS",
                   f"{rej} corrupt line(s) quarantined" if rej else "")
        df.unpersist()

    if rejected_files:
        raise ValueError("; ".join(rejected_files))
    return dict(rows_in=n_in, rows_out=n_in - n_bad, rows_rejected=n_bad)
