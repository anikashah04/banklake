"""Unified source ingestion: three source databases -> extract -> Bronze transformations -> Bronze tables.

One call, `run(business_date)`, handles every table that is due on that date (see sources.due_loads):
  1. extract      read the records from the table's source database
  2. transform    TRIM + UPPER on the configured flag columns, then PII masking with the project's existing
                  AES-256-GCM vault (vault.protect_pandas); add the Bronze metadata columns
  3. insert       write the Bronze Parquet table (same layout the Silver Merge stage already reads)
Everything is driven by metadata (source_tables, bronze_transform_rules, masking_metadata). Each table is a
unit of work: one failing table is logged and reported, the others still load. Re-running the same business date
loads nothing twice (idempotency key = table + load period; the Bronze file is replaced, never appended twice).
All counts and statuses are taken from what each stage actually did and are stored in ingestion_batches.
"""
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import meta, sources, vault
from .common import layer_path
from .schemas import ENTITIES, SOURCES, source_columns

CHUNK = 500
STAGES = ("extract", "transform", "insert")
META_COLS = ["_source_file", "_batch_id", "_ingest_ts", "_load_type"]


# ---------------------------------------------------------------- live progress
@dataclass
class TableState:
    source_id: str
    source_name: str
    entity: str
    frequency: str
    period_key: str
    period_end: str
    stages: dict = field(default_factory=lambda: {s: "waiting" for s in STAGES})   # waiting|processing|done|failed|skipped
    status: str = "WAITING"          # WAITING | RUNNING | SUCCESS | FAILED | SKIPPED
    source_count: int = 0
    extracted: int = 0
    transformed: int = 0
    inserted: int = 0
    bronze_count: int | None = None
    batch_id: int | None = None
    failed_stage: str | None = None
    message: str = ""


class Progress:
    """Shared by the worker thread (writes) and the page (reads). Updated only when a stage really starts/ends."""

    def __init__(self, run_id: str, business_date: str, loads):
        self.lock = threading.RLock()
        self.run_id, self.business_date = run_id, str(business_date)
        self.status = "RUNNING"          # RUNNING | SUCCESS | PARTIAL | FAILED
        self.current_source = self.current_table = self.current_stage = None
        self.tables = [TableState(l.source_id, l.source_name, l.entity, l.frequency, l.period_key, l.period_end) for l in loads]
        self.error = ""

    def snapshot(self) -> dict:
        with self.lock:
            return {"run_id": self.run_id, "business_date": self.business_date, "status": self.status,
                    "current_source": self.current_source, "current_table": self.current_table,
                    "current_stage": self.current_stage, "error": self.error,
                    "tables": [dict(t.__dict__, stages=dict(t.stages)) for t in self.tables]}


_CURRENT: Progress | None = None
_THREAD: threading.Thread | None = None


def running() -> bool:
    return _THREAD is not None and _THREAD.is_alive()


def current() -> dict | None:
    """The live run if there is one in this process, else the latest run rebuilt from the logs."""
    if _CURRENT is not None:
        return _CURRENT.snapshot()
    return latest_from_db()


def latest_from_db() -> dict | None:
    r = meta.query("SELECT * FROM ingestion_runs ORDER BY rowid DESC LIMIT 1")
    if not r:
        return None
    r = r[0]
    tabs = []
    for b in meta.query("SELECT * FROM ingestion_batches WHERE run_id=? ORDER BY id", (r["run_id"],)):
        st = {s: "done" for s in STAGES}
        if b["status"] == "FAILED":
            fs = b["failed_stage"] or "extract"
            hit = False
            for s in STAGES:
                if s == fs:
                    st[s], hit = "failed", True
                elif hit:
                    st[s] = "waiting"
        elif b["status"] == "SKIPPED":
            st = {s: "skipped" for s in STAGES}
        name = SOURCES[b["source_id"]]["name"]
        tabs.append(dict(source_id=b["source_id"], source_name=name, entity=b["entity"], frequency=b["load_frequency"],
                         period_key=b["period_key"], period_end=b["period_end"], stages=st, status=b["status"],
                         source_count=b["source_count"] or 0, extracted=b["extracted"] or 0, transformed=b["transformed"] or 0,
                         inserted=b["inserted"] or 0, bronze_count=b["bronze_count"], batch_id=b["batch_id"],
                         failed_stage=b["failed_stage"], message=b["message"] or ""))
    return {"run_id": r["run_id"], "business_date": r["business_date"], "status": r["status"], "current_source": None,
            "current_table": None, "current_stage": None, "error": "", "tables": tabs}


def arrow_states(snap: dict | None) -> dict:
    """State of every arrow in the pipeline diagram, computed from the per-table stage states (backend truth).
    waiting = nothing started   processing = started, not finished   done = finished   failed = a table failed here
    idle = nothing was due for this part of the pipeline."""
    out = {"S1": "waiting", "S2": "waiting", "S3": "waiting", "extract": "waiting", "transform": "waiting", "insert": "waiting"}
    if not snap:
        return out
    live = snap["status"] == "RUNNING"

    def agg(tables, stage):
        tables = [t for t in tables if t["stages"][stage] != "skipped"]
        if not tables:
            return "idle"
        s = [t["stages"][stage] for t in tables]
        if "failed" in s:
            return "failed"
        if all(x == "done" for x in s):
            return "done"
        if "processing" in s or (live and "done" in s):
            return "processing"
        return "waiting"

    for sid in SOURCES:
        out["S" + sid[-1]] = agg([t for t in snap["tables"] if t["source_id"] == sid], "extract")
    # a stage only concerns tables that got that far (a table that failed earlier is not "waiting" here)
    for stage, key in (("transform", "transform"), ("insert", "insert")):
        prev = STAGES[STAGES.index(stage) - 1]
        reached = [t for t in snap["tables"] if t["stages"][prev] in ("done",) or t["stages"][stage] in ("processing", "done", "failed")]
        out[key] = agg(reached, stage) if reached else ("waiting" if live else "idle")
    # the "extract" arrow into the Transformations box = all extraction finished
    out["extract"] = agg(snap["tables"], "extract")
    return out


# ---------------------------------------------------------------- one table
def _rules(entity: str) -> list[str]:
    return [r["column_name"] for r in meta.query(
        "SELECT column_name FROM bronze_transform_rules WHERE entity=? AND is_active='Y' AND rule='TRIM_UPPER'", (entity,))]


def _bronze_dir(entity: str) -> Path:
    return layer_path("bronze", entity)


def _bronze_file(load: sources.Load) -> str:
    return f"ing-{load.period_key.replace(':', '-')}.parquet"


def _next_batch_id() -> int:
    last = meta.query("SELECT COALESCE(max(batch_id),0) m FROM ingestion_batches")[0]["m"]
    return max(int(time.time() * 1000), int(last) + 1)


def _already_loaded(load: sources.Load):
    r = meta.query("SELECT * FROM ingestion_batches WHERE entity=? AND load_frequency=? AND period_key=? AND status IN ('SUCCESS')"
                   " ORDER BY id DESC LIMIT 1", (load.entity, load.frequency, load.period_key))
    if r and ((_bronze_dir(load.entity) / _bronze_file(load)).exists() or r[0]["bronze_count"] == 0):
        return r[0]
    return None


def _set(ts: TableState, prog: Progress, **kw):
    with prog.lock:
        for k, v in kw.items():
            setattr(ts, k, v)


def _stage(ts, prog, stage, state, pace=0.0):
    with prog.lock:
        ts.stages[stage] = state
        if state == "processing":
            prog.current_source, prog.current_table, prog.current_stage = ts.source_id, ts.entity, stage
    if state == "processing" and pace:
        time.sleep(pace)


def _process(load: sources.Load, ts: TableState, prog: Progress, run_id: str, bd: str, pace: float, hooks: dict):
    """Extract -> transform -> insert one table. Raises on failure (the caller records it)."""
    entity = load.entity
    ts.status = "RUNNING"
    t0 = meta.now()
    bid = meta.execute(
        "INSERT INTO ingestion_batches(run_id,business_date,source_id,entity,load_frequency,period_key,period_end,status,start_ts) "
        "VALUES(?,?,?,?,?,?,?,'RUNNING',?)", (run_id, bd, load.source_id, entity, load.frequency, load.period_key, load.period_end, t0))
    stage = "extract"

    def log(**kw):
        sets = ",".join(f"{k}=?" for k in kw)
        meta.execute(f"UPDATE ingestion_batches SET {sets} WHERE id=?", (*kw.values(), bid))

    try:
        prev = _already_loaded(load)
        if prev:   # idempotent: this table and period are already in Bronze
            for s in STAGES:
                ts.stages[s] = "skipped"
            _set(ts, prog, status="SKIPPED", source_count=prev["source_count"], extracted=prev["extracted"], transformed=prev["transformed"],
                 inserted=prev["inserted"], bronze_count=prev["bronze_count"], batch_id=prev["batch_id"],
                 message=f"already loaded in batch {prev['batch_id']}; nothing inserted again")
            log(status="SKIPPED", batch_id=prev["batch_id"], source_count=prev["source_count"], extracted=prev["extracted"],
                transformed=prev["transformed"], inserted=0, bronze_count=prev["bronze_count"], message=ts.message, end_ts=meta.now())
            return

        # ---------------- 1. extract
        _stage(ts, prog, "extract", "processing", pace)
        hooks.get("extract", lambda *_: None)(load)
        cols = source_columns(entity)
        con = sources.connect(load.source_id)
        try:
            have = [r[1] for r in con.execute(f'PRAGMA table_info("{entity}")')]
            missing = [c for c in cols + ["business_date"] if c not in have]
            if not have:
                raise ValueError(f"source table {entity} does not exist in {SOURCES[load.source_id]['db']}")
            if missing:
                raise ValueError(f"schema mismatch in source table {entity}: missing columns {missing}")
            where, args = sources.where_clause(load)
            total = con.execute(f'SELECT count(*) FROM "{entity}" WHERE {where}', args).fetchone()[0]
            _set(ts, prog, source_count=total)
            cur = con.execute(f'SELECT {",".join(chr(34) + c + chr(34) for c in cols)} FROM "{entity}" WHERE {where} ORDER BY rowid', args)
            frames = []
            while True:
                rows = cur.fetchmany(CHUNK)
                if not rows:
                    break
                frames.append(pd.DataFrame(rows, columns=cols))
                _set(ts, prog, extracted=ts.extracted + len(rows))
        finally:
            con.close()
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)
        if len(df) != total:
            raise RuntimeError(f"extracted {len(df)} of {total} records")
        _stage(ts, prog, "extract", "done")
        log(source_count=total, extracted=len(df))

        # ---------------- 2. Bronze transformations (before anything is written)
        stage = "transform"
        _stage(ts, prog, "transform", "processing", pace)
        hooks.get("transform", lambda *_: None)(load)
        flags = [c for c in _rules(entity) if c in df.columns]
        batch_id = _next_batch_id()
        _set(ts, prog, batch_id=batch_id)
        load_type = "FULL" if load.snapshot else "INCREMENTAL"
        parts = []
        for i in range(0, len(df), CHUNK):
            ch = df.iloc[i:i + CHUNK].copy()
            for c in flags:                                   # TRIM + UPPER on the configured flag columns
                ch[c] = ch[c].map(lambda v: v.strip().upper() if isinstance(v, str) else v)
            ch = vault.protect_pandas(ch, entity, actor="bronze", action="MASK_AT_BRONZE", audit=False)   # AES masking (existing vault)
            parts.append(ch)
            _set(ts, prog, transformed=ts.transformed + len(ch))
        out = pd.concat(parts, ignore_index=True) if parts else df.copy()
        if len(df) == 0:
            for c in sorted(vault_cols(entity)):
                out[c] = pd.Series(dtype=object)
        if len(df):
            from .masking import masking_rules
            meta.log_security("bronze", "MASK_AT_BRONZE", entity, ", ".join(f"{c}:{r}" for c, r in masking_rules(entity).items()))
        out["_source_file"] = f"{SOURCES[load.source_id]['db']}:{entity}:{load.period_key}"
        out["_batch_id"] = batch_id
        out["_ingest_ts"] = pd.Timestamp.now("UTC").tz_localize(None)
        out["_load_type"] = load_type
        tokens = [c for c in out.columns if c.endswith("_token")]
        out = out[cols + META_COLS + tokens]
        _stage(ts, prog, "transform", "done")
        log(batch_id=batch_id, transformed=len(out))

        # ---------------- 3. insert into Bronze
        stage = "insert"
        _stage(ts, prog, "insert", "processing", pace)
        hooks.get("insert", lambda *_: None)(load)
        n = write_bronze(out, load)
        _set(ts, prog, inserted=n)
        back = pq.read_metadata(_bronze_dir(entity) / _bronze_file(load)).num_rows if n else 0   # read back what is really there
        if back != len(out):
            raise RuntimeError(f"Bronze holds {back} rows after inserting {len(out)}")
        _set(ts, prog, bronze_count=back)
        _stage(ts, prog, "insert", "done")
        _set(ts, prog, status="SUCCESS", message="")
        log(status="SUCCESS", inserted=n, bronze_count=back, end_ts=meta.now())
        _pipeline_log(run_id, load, ts, "SUCCESS", t0)
        meta.execute(
            "INSERT INTO ingestion_audit_log(run_id,batch_id,entity,source_file,source_system,load_type,file_hash,records_read,"
            "records_loaded,records_rejected,status,message,ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, batch_id, entity, f"{SOURCES[load.source_id]['db']}:{entity}:{load.period_key}", SOURCES[load.source_id]["system"],
             load_type, None, total, back, 0, "SUCCESS", f"{load.label} load", meta.now()))
    except Exception as e:  # noqa: BLE001 - table-level failure: record it, keep the run going
        msg = f"{type(e).__name__}: {e}"
        with prog.lock:
            for s in STAGES:
                if ts.stages[s] == "processing":
                    ts.stages[s] = "failed"
            ts.status, ts.failed_stage, ts.message = "FAILED", stage, msg
        log(status="FAILED", failed_stage=stage, message=msg[:500], end_ts=meta.now())
        _pipeline_log(run_id, load, ts, "FAILED", t0, msg)
        meta.execute("INSERT INTO ingestion_audit_log(run_id,batch_id,entity,source_file,source_system,load_type,records_read,"
                     "records_loaded,records_rejected,status,message,ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                     (run_id, ts.batch_id, entity, f"{SOURCES[load.source_id]['db']}:{entity}:{load.period_key}",
                      SOURCES[load.source_id]["system"], "FULL" if load.snapshot else "INCREMENTAL", ts.source_count, 0, 0,
                      "FAILED", msg[:300], meta.now()))


def _pipeline_log(run_id, load, ts, status, t0, err=None):
    """Record the Bronze step in pipeline_execution_log so the orchestrator's dependency check (Silver Merge only runs
    after its Bronze pipeline succeeded) works for tables loaded by this ingestion, and a failed table blocks its downstream."""
    if load.entity not in ENTITIES:   # source-only tables have no Silver pipeline to unblock
        return
    meta.execute(
        "INSERT INTO pipeline_execution_log(run_id,pipeline_id,source_table,target_table,start_ts,end_ts,duration_s,rows_in,rows_out,"
        "rows_rejected,status,error_message,retry_count,run_type,layer,entity,batch_id,failure_reason) "
        "VALUES(?,?,?,?,?,?,?,?,?,0,?,?,0,'INGEST','bronze',?,?,?)",
        (run_id, f"bronze_{load.entity}", f"{SOURCES[load.source_id]['db']}:{load.entity}", load.entity, t0, meta.now(),
         round(time.time() - time.mktime(time.strptime(t0, "%Y-%m-%d %H:%M:%S")), 2) if len(t0) == 19 else 0,
         ts.source_count, ts.bronze_count or 0, status, (err or None) and err[:500], load.entity, ts.batch_id,
         None if status == "SUCCESS" else "INGESTION_FAILURE"))


def vault_cols(entity: str) -> list[str]:
    from .masking import masking_rules
    return [f"{c}_token" for c in masking_rules(entity)]


_SCHEMA_TYPES = {"_batch_id": pa.int64(), "_ingest_ts": pa.timestamp("us")}


def write_bronze(df: pd.DataFrame, load: sources.Load) -> int:
    """Write the Bronze Parquet file for one load. The file name is derived from the load period, so inserting the
    same period again REPLACES the file (never duplicates). A snapshot load replaces the table's earlier contents."""
    d = _bronze_dir(load.entity)
    d.mkdir(parents=True, exist_ok=True)
    if len(df) == 0 and not load.snapshot:
        (d / _bronze_file(load)).unlink(missing_ok=True)
        return 0
    final = d / _bronze_file(load)
    tmp = d / f".{final.name}.tmp"
    fields = [pa.field(c, _SCHEMA_TYPES.get(c, pa.string())) for c in df.columns]
    table = pa.Table.from_pandas(df.astype(object).where(df.notna(), None), schema=pa.schema(fields), preserve_index=False) if len(df) \
        else pa.Table.from_pylist([], schema=pa.schema(fields))
    pq.write_table(table, tmp)
    if load.snapshot:      # a closing snapshot is the whole table: remove what was there before
        for old in list(d.iterdir()):
            if old != tmp and not old.name.startswith("."):
                old.unlink() if old.is_file() else __import__("shutil").rmtree(old)
    os.replace(tmp, final)
    if len(df) == 0:
        final.unlink(missing_ok=True)
    return len(df)


# ---------------------------------------------------------------- one run
def _overall(tables) -> str:
    st = [t.status for t in tables]
    if not st or all(s == "SKIPPED" for s in st):
        return "SUCCESS"
    if "FAILED" in st:
        return "FAILED" if all(s == "FAILED" for s in st) else "PARTIAL"
    return "SUCCESS"


def run(business_date, pace: float = 0.0, hooks: dict | None = None, progress: Progress | None = None) -> dict:
    """Ingest every table due on `business_date` from all three sources into Bronze. Blocking."""
    global _CURRENT
    meta.init_db()
    sources.ensure_sources()
    bd = str(sources.check_business_date(business_date))
    loads = sources.due_loads(bd)
    run_id = "ING-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
    prog = progress or Progress(run_id, bd, loads)
    prog.run_id = run_id
    _CURRENT = prog
    meta.execute("INSERT INTO ingestion_runs(run_id,business_date,status,start_ts,tables_total) VALUES(?,?,?,?,?)",
                 (run_id, bd, "RUNNING", meta.now(), len(loads)))
    vault.migrate_legacy()
    try:
        for load, ts in zip(loads, prog.tables):
            _process(load, ts, prog, run_id, bd, pace, hooks or {})
    except Exception as e:  # noqa: BLE001 - never leave the run marked RUNNING
        prog.error = f"{type(e).__name__}: {e}"
    with prog.lock:
        prog.status = "FAILED" if prog.error else _overall(prog.tables)
        prog.current_source = prog.current_table = prog.current_stage = None
        ok = sum(t.status == "SUCCESS" for t in prog.tables)
        bad = sum(t.status == "FAILED" for t in prog.tables)
        skip = sum(t.status == "SKIPPED" for t in prog.tables)
    meta.execute("UPDATE ingestion_runs SET status=?,end_ts=?,tables_ok=?,tables_failed=?,tables_skipped=? WHERE run_id=?",
                 (prog.status, meta.now(), ok, bad, skip, run_id))
    return prog.snapshot()


def start(business_date, pace: float = 0.0) -> bool:
    """Run in a background thread (the page polls current()). False if a run is already going."""
    global _THREAD, _CURRENT
    if running():
        return False
    meta.init_db()
    sources.ensure_sources()
    bd = str(sources.check_business_date(business_date))
    prog = Progress("starting", bd, sources.due_loads(bd))
    _CURRENT = prog
    _THREAD = threading.Thread(target=run, kwargs=dict(business_date=bd, pace=pace, progress=prog), daemon=True)
    _THREAD.start()
    return True


# ---------------------------------------------------------------- reading Bronze back
def bronze_table(entity: str, limit: int = 200) -> pd.DataFrame:
    import duckdb
    d = _bronze_dir(entity)
    if not list(d.glob("*.parquet")):
        return pd.DataFrame()
    return duckdb.connect().execute(f"SELECT * FROM read_parquet('{d}/*.parquet') LIMIT {int(limit)}").fetchdf()


def bronze_batch(entity: str, batch_id: int, limit: int = 200) -> pd.DataFrame:
    import duckdb
    d = _bronze_dir(entity)
    if not list(d.glob("*.parquet")):
        return pd.DataFrame()
    return duckdb.connect().execute(f"SELECT * FROM read_parquet('{d}/*.parquet') WHERE _batch_id=? LIMIT {int(limit)}", [int(batch_id)]).fetchdf()


# ---------------------------------------------------------------- dry run of ONE row (no writes anywhere)
def dry_run_steps(entity: str, business_date, nth: int = 1) -> list:
    """The steps one source record goes through, as executable steps: [(title, fn)]. fn(ctx) does the real work of that step on
    the real record (same primitives as run(): the source query, TRIM+UPPER, vault.token_for / vault.encrypt / mask_value) and
    returns {"note", "sql", "table": DataFrame}. Nothing is written: not Bronze, not the vault, not the logs."""
    from .masking import mask_value, masking_rules
    bd = sources.check_business_date(business_date)
    load = next((l for l in sources.due_loads(bd) if l.entity == entity), None)
    if load is None:
        raise ValueError(f"{entity} is not due on {bd:%d %b %Y}")
    cols = source_columns(entity)
    nth = max(1, int(nth))

    def s_due(ctx):
        ctx["load"] = load
        where, args = sources.where_clause(load)
        ctx["where"], ctx["args"] = where, args
        rule = {"DAILY": "records of the business date", "MONTHLY": "previous month's closing snapshot",
                "QUARTERLY": "previous quarter's closing snapshot"}[load.frequency]
        return {"note": f"{entity} loads {load.frequency.lower()}: {rule}.",
                "table": pd.DataFrame({"Item": ["Source database", "Load frequency", "Load period", "Period end", "Selection rule"],
                                       "Value": [SOURCES[load.source_id]["db"], load.frequency.title(), load.period_key, load.period_end,
                                                 f"{where.replace('?', repr(load.period_end))}"]})}

    def s_extract(ctx):
        sql = f'SELECT {", ".join(cols)} FROM "{entity}" WHERE {ctx["where"]} ORDER BY rowid LIMIT 1 OFFSET {nth - 1}'
        con = sources.connect(load.source_id)
        try:
            total = con.execute(f'SELECT count(*) FROM "{entity}" WHERE {ctx["where"]}', ctx["args"]).fetchone()[0]
            row = con.execute(sql.replace("?", "?"), ctx["args"]).fetchone()
        finally:
            con.close()
        if row is None:
            raise ValueError(f"row {nth} does not exist: only {total} records are due for {entity} in this load")
        ctx["raw"] = dict(zip(cols, row))
        ctx["total"] = total
        return {"note": f"Record {nth} of {total:,} due for this load, read from the source database exactly as stored.",
                "sql": sql.replace("?", repr(load.period_end)),
                "table": pd.DataFrame({"Column": cols, "Value in source": [repr(v) for v in row]})}

    def s_trim(ctx):
        flags = [c for c in _rules(entity) if c in ctx["raw"]]
        row = dict(ctx["raw"])
        recs = []
        for c in flags:
            before = row[c]
            row[c] = before.strip().upper() if isinstance(before, str) else before
            recs.append({"Column": c, "Before": repr(before), "After": repr(row[c]), "Changed": "yes" if before != row[c] else "no"})
        ctx["clean"] = row
        return {"note": "TRIM and UPPER are applied to the configured flag columns." if flags else "No flag columns are configured for this table.",
                "table": pd.DataFrame(recs) if recs else None}

    def s_mask(ctx):
        from . import vault
        rules = {c: r for c, r in masking_rules(entity).items() if c in ctx["clean"]}
        row, recs = dict(ctx["clean"]), []
        ctx["tokens"] = {}
        for c, rule in rules.items():
            v = row[c]
            if v is None:
                recs.append({"Column": c, "Rule": rule, "Value": "None", "Token": "None", "Masked value": "None", "AES-256-GCM ciphertext": "-"})
                ctx["tokens"][f"{c}_token"] = None
                continue
            tok = vault.token_for(v)
            ct = vault.encrypt(entity, c, tok, str(v)).hex()
            masked = mask_value(rule, v)
            recs.append({"Column": c, "Rule": rule, "Value": repr(v), "Token": tok, "Masked value": repr(masked), "AES-256-GCM ciphertext": ct[:36] + "…"})
            row[c] = masked
            ctx["tokens"][f"{c}_token"] = tok
        ctx["masked"] = row
        return {"note": ("The real value is encrypted for the vault and replaced by a masked value plus a token. A dry run does not store it."
                         if rules else "No PII columns are configured for this table."),
                "table": pd.DataFrame(recs) if recs else None}

    def s_meta(ctx):
        ctx["meta"] = {"_source_file": f"{SOURCES[load.source_id]['db']}:{entity}:{load.period_key}", "_batch_id": _next_batch_id(),
                       "_ingest_ts": str(pd.Timestamp.now("UTC").tz_localize(None)), "_load_type": "FULL" if load.snapshot else "INCREMENTAL"}
        return {"note": "Ingestion metadata added to every Bronze row.",
                "table": pd.DataFrame({"Column": list(ctx["meta"]), "Value": [str(v) for v in ctx["meta"].values()]})}

    def s_final(ctx):
        final = {**ctx["masked"], **ctx["meta"], **ctx["tokens"]}
        ctx["final"] = final
        return {"note": "This is the row that a real ingestion would insert into the Bronze table. Dry run: nothing was written to Bronze, the vault or the logs.",
                "table": pd.DataFrame({"Column": list(final), "Bronze value": [repr(v) for v in final.values()]})}

    return [("Check what is due", s_due), ("Extract the record from the source", s_extract), ("Trim and upper-case flag columns", s_trim),
            ("Mask PII with the AES vault", s_mask), ("Add Bronze metadata", s_meta), ("Row ready for Bronze", s_final)]
