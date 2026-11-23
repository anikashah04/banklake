"""Silver Merge handler: reads new Bronze batches, applies CDC, writes the merged table, keeps a
watermark and writes an audit row. Strategy comes from pipeline_metadata (full / incremental)."""
import os
import shutil

from pyspark.sql import functions as F

from .. import meta
from ..common import layer_path
from ..schemas import ENTITIES
from . import USE_ICEBERG, cdc_processor, exists, iceberg_merge, read


def _parquet_merge(spark, entity, rows, key):
    path = layer_path("silver_merge", entity)
    p = str(path)
    if not path.exists():
        rows.write.mode("overwrite").parquet(p)
        return
    existing = spark.read.parquet(p)
    merged = existing.join(rows.select(key).distinct(), key, "left_anti").unionByName(rows, allowMissingColumns=True)
    tmp = p + "__tmp"
    merged.write.mode("overwrite").parquet(tmp)
    shutil.rmtree(p)
    os.rename(tmp, p)
    spark.catalog.refreshByPath(p)


def run(spark, meta_row, ctx):
    pid, entity = meta_row["pipeline_id"], meta_row["target_table"]
    key = ENTITIES[entity]["key"]
    strategy = "full" if ctx.full_refresh else meta_row["load_strategy"]
    bronze = spark.read.parquet(str(layer_path("bronze", entity)))
    wmcol = meta_row["watermark_column"] or "_batch_id"
    wm = meta.get_watermark(pid) if strategy == "incremental" else -1
    new = bronze.filter(F.col(wmcol) > wm).cache()
    rows_in = new.count()
    if rows_in == 0:
        return dict(rows_in=0, rows_out=0, rows_rejected=0)

    latest, bad = cdc_processor.validate_and_dedupe(new, key)
    latest = latest.cache()
    n_latest, n_bad = latest.count(), bad.count()
    dupes = rows_in - n_bad - n_latest

    incremental = strategy == "incremental" and exists(spark, entity)
    classified = cdc_processor.classify(latest, read(spark, entity) if incremental else None, key).cache()
    counts = {r["_merge_action"]: r["count"] for r in classified.groupBy("_merge_action").count().collect()}
    rows = cdc_processor.to_target_rows(classified, ctx.run_id).drop("_merge_action") \
        .join(classified.select(key, "_merge_action"), key)  # keep the action as a column on the stored row

    if USE_ICEBERG:
        iceberg_merge.merge(spark, entity, rows, key, overwrite=not incremental)
    elif incremental:
        _parquet_merge(spark, entity, rows, key)
    else:
        rows.write.mode("overwrite").parquet(str(layer_path("silver_merge", entity)))

    bad.withColumn("_run_id", F.lit(ctx.run_id)).write.mode("append" if incremental else "overwrite") \
        .parquet(str(layer_path("quarantine", f"silver_merge_{entity}")))
    batch = new.agg(F.max(wmcol)).first()[0]
    meta.set_watermark(pid, batch, ctx.run_id)
    ins, upd, dele = counts.get("INSERT", 0), counts.get("UPDATE", 0), counts.get("DELETE", 0)
    ignored = counts.get("STALE", 0) + counts.get("DELETE_NO_TARGET", 0)
    meta.execute(
        "INSERT INTO transformation_audit_log(run_id,pipeline_id,layer,entity,batch_id,load_type,rows_in,inserts,updates,"
        "deletes,ignored,duplicates_removed,rejected,ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (ctx.run_id, pid, "silver_merge", entity, batch, strategy.upper(), rows_in, ins, upd, dele, ignored, dupes, n_bad, meta.now()))
    return dict(rows_in=rows_in, rows_out=ins + upd + dele, rows_rejected=n_bad)
