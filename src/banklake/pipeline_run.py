"""Run the whole pipeline for one date in the background: Ingestion (history catch-up) -> Silver (merge, then transform) -> Gold.
It uses the same ingest.start and engine.start the Ingestion and Transformation pages use, one after the other, so the audit trail is identical.
No SQL INSERT/UPDATE/MERGE: ingestion writes Bronze, the transformation scripts SELECT, delete the report date and append."""
import threading
import time
from datetime import date

from . import ingest, meta, sources
from .transform import engine

_lock = threading.Lock()
_state: dict = {"run": None}
_thread: threading.Thread | None = None


def running() -> bool:
    return _thread is not None and _thread.is_alive()


def busy() -> bool:
    """Anything that writes to the lake is going on (this run, or a single Ingestion or Transformation run)."""
    return running() or ingest.running() or engine.running()


def current() -> dict | None:
    with _lock:
        r = _state["run"]
        return dict(r) if r else None


def _set(**kw):
    with _lock:
        _state["run"].update(kw)


def _wait(alive):
    while alive():
        time.sleep(0.3)


def _work(rd: str, skip_ingest: bool):
    try:
        if not skip_ingest:
            _set(stage="ingest")
            if not ingest.start(rd, pace=0.0, history=True):
                raise ValueError("An ingestion is already running.")
            _wait(ingest.running)
            snap = ingest.current()
            if snap["status"] == "FAILED" or not any(t["status"] in ("SUCCESS", "SKIPPED") for t in snap["tables"]):
                raise RuntimeError(snap.get("error") or "Ingestion failed, so nothing could be transformed.")
            _set(ingest_status=snap["status"], ingest_run=snap["run_id"])
        else:
            _set(ingest_status="SKIPPED")
        results = []
        for stage, layer in (("silver", "silver_transform"), ("gold", "gold")):
            _set(stage=stage)
            try:
                engine.start(layer, rd, None)         # the whole layer; its unloaded upstream tables (Silver Merge for Silver) run first
            except ValueError as e:
                raise RuntimeError(str(e)) from e
            _wait(engine.running)
            run = engine.current()
            results.append(run)
            if stage == "silver":
                _set(silver_items=[dict(i) for i in run["items"]])
        items = [i for r in results for i in r["items"]]
        bad = [i for i in items if i["status"] != "SUCCESS"]
        skipped = sorted({t for r in results for t in r["skipped"]})
        gold_ok = sum(i["status"] == "SUCCESS" for i in results[1]["items"])
        status = "SUCCESS" if not bad and not skipped and _state["run"]["ingest_status"] in ("SUCCESS", "SKIPPED") else "PARTIAL" if gold_ok else "FAILED"
        _set(stage="done", status=status, order_id=results[1]["order_id"], failed=[i["target"] for i in bad], skipped=skipped, ended=time.time())
    except Exception as e:  # noqa: BLE001 - the page must show the reason, never a stuck spinner
        _set(stage="done", status="FAILED", error=f"{e}", ended=time.time())


def start(report_date) -> None:
    """Start the background run. Raises ValueError for a bad date or when something else is already running."""
    global _thread
    meta.init_db()
    sources.ensure_sources()
    rd = sources.check_business_date(report_date)           # ValueError for a future or malformed date
    if busy():
        raise ValueError("Another run is in progress. Wait for it to finish.")
    r = meta.query("SELECT max(business_date) d FROM ingestion_runs WHERE tables_ok > 0 OR tables_skipped > 0")
    through = date.fromisoformat(str(r[0]["d"])[:10]) if r and r[0]["d"] else None
    skip = through is not None and rd <= through             # Bronze already holds this date; loading an earlier snapshot would roll it back
    with _lock:
        _state["run"] = dict(report_date=rd.isoformat(), stage="ingest" if not skip else "silver", status="RUNNING", started=time.time(), ended=None,
                             error=None, ingest_status=None, silver_items=[], ingest_skipped=skip, failed=[], skipped=[], order_id=None)
    _thread = threading.Thread(target=_work, args=(rd.isoformat(), skip), daemon=True)
    _thread.start()
