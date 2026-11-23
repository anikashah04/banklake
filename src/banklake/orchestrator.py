"""Metadata-driven orchestrator: reads pipeline_metadata, resolves dependencies, runs handlers
by name, retries failures, blocks downstream pipelines and writes an audit trail."""
import importlib
import time
import traceback
import uuid
from dataclasses import dataclass, field

from . import meta
from .monitor import classify_failure
from .common import get_spark


@dataclass
class RunContext:
    run_id: str = field(default_factory=lambda: time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6])
    full_refresh: bool = False
    retry_sleep: float = 1.0
    run_type: str = "RUN"  # RUN or RELOAD


def resolve(handler: str):
    module, func = handler.rsplit(".", 1)
    return getattr(importlib.import_module(module), func)


def topo_sort(ids: list[str], deps: dict[str, list[str]], priority: dict[str, int] | None = None) -> list[str]:
    """Kahn's algorithm limited to the selected pipelines. Dependencies decide what may run; among pipelines
    that are ready at the same time, the metadata column execution_order decides who goes first."""
    priority = priority or {}
    remaining, order = set(ids), []
    while remaining:
        ready = sorted((p for p in remaining if not (set(deps.get(p, [])) & remaining)),
                       key=lambda p: (priority.get(p, 1_000_000), p))
        if not ready:
            raise ValueError(f"Circular dependency among {sorted(remaining)}")
        order += ready
        remaining -= set(ready)
    return order


def _tag(exec_id: int, m: dict, status: str, message: str | None = None) -> None:
    """Fill the audit context for a step: layer, entity, the batch it had processed (the watermark) and a failure category."""
    wm = meta.get_watermark(m["pipeline_id"])  # -1 = no batch processed yet -> NULL in the log
    meta.execute("UPDATE pipeline_execution_log SET layer=?, entity=?, batch_id=?, failure_reason=? WHERE execution_id=?",
                 (m["target_layer"], m["target_table"], None if wm < 0 else wm, classify_failure(status, message), exec_id))


def _last_status(pid: str):
    r = meta.query("SELECT status FROM pipeline_execution_log WHERE pipeline_id=? ORDER BY execution_id DESC LIMIT 1", (pid,))
    return r[0]["status"] if r else None


def downstream_of(pipelines: list[str]) -> list[str]:
    """The given pipelines plus every pipeline that (transitively) depends on them."""
    rev: dict[str, list[str]] = {}
    for d in meta.query("SELECT * FROM pipeline_dependencies"):
        rev.setdefault(d["depends_on"], []).append(d["pipeline_id"])
    seen, stack = set(pipelines), list(pipelines)
    while stack:
        for child in rev.get(stack.pop(), []):
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return sorted(seen)


def reload(pipelines: list[str], downstream: bool = True, retry_sleep=1.0, spark=None) -> dict:
    """Rebuild pipelines from scratch: full refresh (watermarks are reset by the successful run) of the
    selected pipelines and, by default, everything that depends on them. Logged as run_type RELOAD."""
    known = {r["pipeline_id"] for r in meta.query("SELECT pipeline_id FROM pipeline_metadata")}
    unknown = [p for p in pipelines if p not in known]
    if unknown:
        raise ValueError(f"unknown pipeline(s): {unknown}")
    chosen = downstream_of(pipelines) if downstream else list(pipelines)
    return run(pipelines=chosen, full_refresh=True, retry_sleep=retry_sleep, spark=spark, run_type="RELOAD")


def run(layers=None, pipelines=None, full_refresh=False, retry_sleep=1.0, spark=None, run_type="RUN") -> dict:
    ctx = RunContext(full_refresh=full_refresh, retry_sleep=retry_sleep, run_type=run_type)
    rows = meta.query("SELECT * FROM pipeline_metadata")
    if layers:
        rows = [r for r in rows if r["target_layer"] in layers]
    if pipelines:
        rows = [r for r in rows if r["pipeline_id"] in pipelines]
    by_id = {r["pipeline_id"]: r for r in rows}
    deps = {}
    for d in meta.query("SELECT * FROM pipeline_dependencies"):
        deps.setdefault(d["pipeline_id"], []).append(d["depends_on"])
    status: dict[str, str] = {}
    summary = {}

    for pid in topo_sort(list(by_id), deps, {p: r["execution_order"] for p, r in by_id.items() if r["execution_order"] is not None}):
        m = by_id[pid]
        base = (ctx.run_id, pid, m["source_table"], m["target_table"], meta.now())
        if m["is_active"] != "Y":
            eid = meta.execute("INSERT INTO pipeline_execution_log(run_id,pipeline_id,source_table,target_table,start_ts,end_ts,duration_s,rows_in,rows_out,rows_rejected,status,error_message,retry_count,run_type) "
                               "VALUES(?,?,?,?,?,?,0,0,0,0,'SKIPPED','pipeline is inactive',0,?)", (*base, meta.now(), ctx.run_type))
            _tag(eid, m, "SKIPPED")
            status[pid] = summary[pid] = "SKIPPED"
            continue
        blocked = [d for d in deps.get(pid, [])
                   if status.get(d) in ("FAILED", "BLOCKED") or (d not in status and _last_status(d) not in ("SUCCESS", "SKIPPED"))]
        if blocked:
            eid = meta.execute("INSERT INTO pipeline_execution_log(run_id,pipeline_id,source_table,target_table,start_ts,end_ts,duration_s,rows_in,rows_out,rows_rejected,status,error_message,retry_count,run_type) "
                               "VALUES(?,?,?,?,?,?,0,0,0,0,'BLOCKED',?,0,?)", (*base, meta.now(), f"upstream not successful: {blocked}", ctx.run_type))
            _tag(eid, m, "BLOCKED")
            status[pid] = summary[pid] = "BLOCKED"
            continue

        exec_id = meta.execute("INSERT INTO pipeline_execution_log(run_id,pipeline_id,source_table,target_table,start_ts,status,run_type) VALUES(?,?,?,?,?,'RUNNING',?)", (*base, ctx.run_type))
        t0, attempt, result, err = time.time(), 0, None, None
        max_try = 1 + int(m["max_retries"] or 0)
        while attempt < max_try:
            attempt += 1
            try:
                result = resolve(m["handler"])(spark or get_spark(), m, ctx)
                err = None
                break
            except Exception as e:  # noqa: BLE001 - we want to log everything and retry
                err = f"{type(e).__name__}: {e}"
                meta.execute("INSERT INTO pipeline_error_log(run_id,pipeline_id,ts,error) VALUES(?,?,?,?)",
                             (ctx.run_id, pid, meta.now(), err + "\n" + traceback.format_exc(limit=3)))
                if attempt < max_try:
                    time.sleep(ctx.retry_sleep * attempt)
        dur = round(time.time() - t0, 2)
        if err is None:
            meta.execute("UPDATE pipeline_execution_log SET end_ts=?,duration_s=?,rows_in=?,rows_out=?,rows_rejected=?,status='SUCCESS',retry_count=? WHERE execution_id=?",
                         (meta.now(), dur, result["rows_in"], result["rows_out"], result["rows_rejected"], attempt - 1, exec_id))
            _tag(exec_id, m, "SUCCESS")
            status[pid] = summary[pid] = "SUCCESS"
        else:
            meta.execute("UPDATE pipeline_execution_log SET end_ts=?,duration_s=?,status='FAILED',error_message=?,retry_count=? WHERE execution_id=?",
                         (meta.now(), dur, err[:500], attempt - 1, exec_id))
            _tag(exec_id, m, "FAILED", err)
            status[pid] = summary[pid] = "FAILED"
    return {"run_id": ctx.run_id, "status": summary}
