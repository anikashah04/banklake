"""Runs the procedures: Teradata -> Athena (convert) -> EMR script (generate) -> run (load the target table).

A run happens in a background thread so the page can show real progress. Every step writes a row to layer_audit_log
(order_id, layer, source, target, step_id, status, timestamp, retry_count, input/output counts, error).
"""
import os
import threading
import time
import uuid
from datetime import datetime

import duckdb

from .. import meta
from ..common import DATA, layer_path
from . import catalog, convert, runtime

MAX_RETRIES = 1
_lock = threading.Lock()
_state = {"run": None}
_thread = None

TERADATA_DIR, ATHENA_DIR, EMR_DIR = DATA / "transform_sql" / "teradata", DATA / "transform_sql" / "athena", DATA / "emr_scripts"


# ------------------------------------------------------------------ convert + generate
def _artifact(pid):
    r = meta.query("SELECT * FROM proc_artifacts WHERE proc_id = ?", (pid,))
    return r[0] if r else None


def to_athena(pid):
    """'Convert to Athena': Teradata SELECT -> Athena SELECT. Returns the stored artifact row."""
    p = catalog.BY_ID[pid]
    a = convert.to_athena(p)
    TERADATA_DIR.mkdir(parents=True, exist_ok=True)
    ATHENA_DIR.mkdir(parents=True, exist_ok=True)
    (TERADATA_DIR / f"{p['target']}.sql").write_text(p["teradata"] + ";\n")
    if a["status"] == "CONVERTED":
        (ATHENA_DIR / f"{p['target']}.sql").write_text(a["sql"] + ";\n")
    notes = "\n".join(a["notes"] + ["UNSUPPORTED: " + x for x in a["problems"]])
    meta.execute("INSERT INTO proc_artifacts(proc_id,layer,athena_sql,athena_status,athena_notes,athena_ts) VALUES(?,?,?,?,?,?) "
                 "ON CONFLICT(proc_id) DO UPDATE SET athena_sql=excluded.athena_sql, athena_status=excluded.athena_status, "
                 "athena_notes=excluded.athena_notes, athena_ts=excluded.athena_ts, emr_sql=NULL, script=NULL, script_ts=NULL",
                 (pid, p["layer"], a["sql"], a["status"], notes, meta.now()))
    return _artifact(pid)


def generate_emr(pid):
    """'Generate EMR': Athena SELECT -> Spark SQL -> PySpark script (file under data/emr_scripts/<layer>/)."""
    art = _artifact(pid)
    if not art or art["athena_status"] != "CONVERTED":
        raise ValueError("Convert the procedure to Athena first.")
    p = catalog.BY_ID[pid]
    spark_sql = convert.to_spark(art["athena_sql"])
    script = convert.build_script(p, spark_sql)
    compile(script, p["script_name"] + ".py", "exec")
    path = EMR_DIR / p["layer"] / f"{p['script_name']}.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(script)
    meta.execute("UPDATE proc_artifacts SET emr_sql=?, script=?, script_path=?, script_ts=? WHERE proc_id=?",
                 (spark_sql, script, f"emr_scripts/{p['layer']}/{p['script_name']}.py", meta.now(), pid))
    return _artifact(pid)


def convert_all(pid):
    """One click: Teradata -> Athena -> Spark SQL -> EMR script. Returns the stored artifact row."""
    art = to_athena(pid)
    return generate_emr(pid) if art["athena_status"] == "CONVERTED" else art


def artifacts(pid):
    return _artifact(pid) or {}


# ------------------------------------------------------------------ audit helpers
def _audit(order, layer, source, target, status, retry, n_in, n_out, error, report_date):
    runtime.audit(order, layer, source, target, runtime.next_step(order), status, retry, n_in, n_out, error, report_date)


def loaded(target, layer, report_date):
    """Latest successful load of a target table for a report date, or None."""
    r = meta.query("SELECT output_count, input_count, timestamp, order_id FROM layer_audit_log WHERE layer=? AND target_table_name=? "
                   "AND report_date=? AND status='SUCCESS' ORDER BY id DESC LIMIT 1", (layer, target, report_date))
    return r[0] if r else None


def blocked_by(pid, report_date):
    """Source tables that have not been loaded yet (empty list = ready)."""
    p = catalog.BY_ID[pid]
    missing = []
    for s in p["sources"]:
        if p["source_db"] == "bronze":
            if not any(layer_path("bronze", s).glob("*.parquet")):
                missing.append(f"bronze.{s}")
        else:
            prev = catalog.LAYERS[catalog.LAYERS.index(p["layer"]) - 1]
            if not loaded(s, prev, report_date):
                missing.append(f"{p['source_db']}.{s}")
    return missing


def block_message(missing, report_date):
    """Plain wording for tables that are not ready: Bronze needs Ingestion, anything else needs its own transformation first."""
    bronze = [m.split(".", 1)[1] for m in missing if m.startswith("bronze.")]
    other = [m for m in missing if not m.startswith("bronze.")]
    parts = []
    if bronze:
        parts.append("No Bronze data yet for " + ", ".join(bronze) + ". Run Ingestion on a date when they are due")
    if other:
        parts.append("Not loaded yet for " + report_date + ": " + ", ".join(other))
    return ". ".join(parts) + "."


def runnable(pid):
    """True when every Bronze table this procedure (and everything upstream of it) reads already holds data."""
    return not any(blocked_by(q["proc_id"], "") for q in catalog.upstream(pid) + [catalog.BY_ID[pid]] if q["source_db"] == "bronze")


# ------------------------------------------------------------------ one procedure
def _args(p, report_date):
    cat = "bl" if runtime.use_iceberg() else "glue_catalog"
    return ["--catalog", cat, "--source_db", p["source_db"], "--target_tables", p["target"], "--report_date", report_date,
            "--secret_name", "local-metadata", "--env", "dev", "--region", "ap-southeast-2", "--script_name", p["script_name"]]


def run_proc(pid, report_date, order_id, spark=None, convert_first=True):
    """Convert (if needed), generate (if needed) and run one procedure. Returns dict(status, rows_in, rows_out, error)."""
    p = catalog.BY_ID[pid]
    art = _artifact(pid)
    if convert_first and (not art or art["athena_status"] != "CONVERTED"):
        art = to_athena(pid)
    if art["athena_status"] != "CONVERTED":
        _audit(order_id, p["layer"], p["source_db"], p["target"], "FAILED", 0, 0, 0, "Athena conversion failed: " + art["athena_notes"][-200:], report_date)
        return dict(status="FAILED", rows_in=0, rows_out=0, error="Athena conversion failed")
    if convert_first and not art["script"]:
        art = generate_emr(pid)
    missing = blocked_by(pid, report_date)
    if missing:
        msg = block_message(missing, report_date)
        _audit(order_id, p["layer"], ",".join(missing), p["target"], "BLOCKED", 0, 0, 0, msg, report_date)
        return dict(status="BLOCKED", rows_in=0, rows_out=0, error=msg)
    if spark is None:
        from ..common import get_spark
        spark = get_spark()
    ns = {"__name__": "emr_job"}
    exec(compile(art["script"], f"<{p['script_name']}.py>", "exec"), ns)  # noqa: S102 - the script was generated by this module
    args = ns["get_args"](_args(p, report_date))
    last = None
    for attempt in range(MAX_RETRIES + 1):
        os.environ["BANKLAKE_ORDER_ID"], os.environ["BANKLAKE_RETRY_COUNT"] = order_id, str(attempt)
        try:
            r = ns["main"](spark, args)
            return dict(status="SUCCESS", rows_in=r["rows_in"], rows_out=r["rows_out"], error=None)
        except Exception as e:  # noqa: BLE001 - the script has already written the FAILED audit row
            last = str(e).splitlines()[0][:300] if str(e) else type(e).__name__
            time.sleep(0.2)
    return dict(status="FAILED", rows_in=0, rows_out=0, error=last)


def new_order_id():
    return datetime.now().strftime("ORD-%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4]


# ------------------------------------------------------------------ background runs (one procedure, or a whole layer)
def running():
    return _thread is not None and _thread.is_alive()


def current():
    with _lock:
        r = _state["run"]
        return None if r is None else dict(r, items=[dict(i) for i in r["items"]])


def start(layer, report_date, targets=None, with_upstream=True):
    """Run procedures in the background. targets=None runs the whole layer (skipping tables whose Bronze data does not exist yet);
    otherwise the listed targets of that layer. Either way any upstream procedure not yet loaded for this report date runs first."""
    global _thread
    with _lock:
        if running():
            raise ValueError("A transformation run is already in progress.")
        procs = [p for p in catalog.procs_for(layer) if targets is None or p["target"] in targets]
        skipped = []
        if targets is None:
            skipped = [p["target"] for p in procs if not runnable(p["proc_id"])]
            procs = [p for p in procs if p["target"] not in skipped]
        chosen = {p["proc_id"] for p in procs}
        if with_upstream:
            ups = {}
            for p in procs:
                for q in catalog.upstream(p["proc_id"]):
                    if q["proc_id"] not in chosen and not loaded(q["target"], q["layer"], report_date):
                        ups[q["proc_id"]] = q
            procs = sorted(list(ups.values()) + procs, key=lambda q: q["order"])
        if not procs:
            raise ValueError("Nothing to run: the Bronze data these tables need has not been ingested yet.")
        order = new_order_id()
        focus = (catalog.BY_TARGET[(layer, sorted(targets)[0])]["proc_id"] if targets else procs[-1]["proc_id"])
        _state["run"] = dict(order_id=order, layer=layer, report_date=report_date, status="RUNNING", started=time.time(), ended=None, error=None,
                             current=None, kind="LAYER" if targets is None else "PROC", targets=sorted(chosen), focus=focus, skipped=skipped,
                             items=[dict(proc_id=p["proc_id"], layer=p["layer"], target=p["target"], stage="Waiting", status="WAITING",
                                         rows_in=0, rows_out=0, error=None) for p in procs])
    _thread = threading.Thread(target=_work, args=(order,), daemon=True)
    _thread.start()
    return order


def _set(item, **kw):
    with _lock:
        item.update(kw)


def _work(order):
    run = _state["run"]
    try:
        from ..common import get_spark
        spark = get_spark()
        for it in run["items"]:
            pid = it["proc_id"]
            with _lock:
                run["current"] = it["target"]
            _set(it, status="RUNNING", stage="Converting to Athena")
            art = to_athena(pid)
            if art["athena_status"] == "CONVERTED":
                _set(it, stage="Generating EMR script")
                generate_emr(pid)
                _set(it, stage="Running on Spark")
            res = run_proc(pid, run["report_date"], order, spark, convert_first=False)
            _set(it, status=res["status"], stage={"SUCCESS": "Loaded", "BLOCKED": "Blocked", "FAILED": "Failed"}[res["status"]],
                 rows_in=res["rows_in"], rows_out=res["rows_out"], error=res["error"])
        with _lock:
            bad = [i for i in run["items"] if i["status"] != "SUCCESS"]
            run.update(status="FAILED" if bad else "SUCCESS", ended=time.time(), current=None)
    except Exception as e:  # noqa: BLE001
        with _lock:
            run.update(status="FAILED", ended=time.time(), error=str(e)[:300], current=None)


# ------------------------------------------------------------------ read side for the page
def layer_overview(layer, report_date):
    """One row per procedure with what has happened so far for this report date."""
    out = []
    for p in catalog.procs_for(layer):
        art = _artifact(p["proc_id"]) or {}
        ld = loaded(p["target"], layer, report_date)
        last = meta.query("SELECT status, error_message, timestamp FROM layer_audit_log WHERE layer=? AND target_table_name=? AND report_date=? "
                          "ORDER BY id DESC LIMIT 1", (layer, p["target"], report_date))
        out.append(dict(proc_id=p["proc_id"], target=p["target"], sources=", ".join(f"{p['source_db']}.{s}" for s in p["sources"]),
                        athena=art.get("athena_status") == "CONVERTED", emr=bool(art.get("script")),
                        rows=ld["output_count"] if ld else None, loaded_at=ld["timestamp"] if ld else None,
                        last_status=last[0]["status"] if last else None, last_error=last[0]["error_message"] if last else None))
    return out


def layer_ready(layer, report_date):
    return all(loaded(p["target"], layer, report_date) for p in catalog.procs_for(layer))


def preview(layer, table, report_date, n=100):
    p = runtime.table_path(layer, table)
    if not any(p.glob("*.parquet")):
        return None
    return duckdb.connect().execute(f"SELECT * FROM read_parquet('{p}/*.parquet') WHERE report_date = DATE '{report_date}' LIMIT {int(n)}").fetchdf()


def audit_rows(order_id=None, layer=None, limit=300):
    sql, par = "SELECT order_id, layer, source_table_name, target_table_name, step_id, status, timestamp, retry_count, input_count, output_count, error_message FROM layer_audit_log", []
    cond = []
    if order_id:
        cond.append("order_id=?")
        par.append(order_id)
    if layer:
        cond.append("layer=?")
        par.append(layer)
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    return meta.query(sql + f" ORDER BY id DESC LIMIT {int(limit)}", tuple(par))
