"""Module 9: audit context (layer, entity, batch_id, failure_reason), monitoring summary and alerts."""
import pytest

import probe_handlers
from banklake import meta, monitor, orchestrator


@pytest.fixture()
def probes():
    made = []

    def add(pid, handler="probe_handlers.ok", retries=0, deps=(), active="Y", layer="bronze"):
        meta.execute("INSERT INTO pipeline_metadata(pipeline_id,source_table,target_table,source_layer,target_layer,load_strategy,"
                     "is_active,handler,max_retries,execution_order) VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (pid, "x", f"ent_{pid}", "raw", layer, "full", active, handler, retries, 1))
        for d in deps:
            meta.execute("INSERT INTO pipeline_dependencies VALUES(?,?)", (pid, d))
        made.append(pid)

    meta.init_db()
    probe_handlers.CALLS.clear()
    yield add
    for pid in made:
        for t in ("pipeline_metadata", "pipeline_execution_log", "pipeline_error_log"):
            meta.execute(f"DELETE FROM {t} WHERE pipeline_id=?", (pid,))
        meta.execute("DELETE FROM pipeline_dependencies WHERE pipeline_id=? OR depends_on=?", (pid, pid))
        meta.execute("DELETE FROM pipeline_watermark WHERE pipeline_id=?", (pid,))


@pytest.mark.parametrize("status,msg,expected", [
    ("SUCCESS", None, None), ("RUNNING", None, None), ("SKIPPED", "inactive", "INACTIVE"),
    ("BLOCKED", "upstream not successful", "UPSTREAM_FAILED"),
    ("FAILED", "ValueError: Schema mismatch in loan.csv: missing ['x']", "SCHEMA_MISMATCH"),
    ("FAILED", "FileNotFoundError: No such file or directory", "MISSING_INPUT"),
    ("FAILED", "AnalysisException: Path does not exist: file:/x", "MISSING_INPUT"),
    ("FAILED", "PermissionError: access denied", "PERMISSION_DENIED"),
    ("FAILED", "ZeroDivisionError: division by zero", "UNEXPECTED_ERROR"),
])
def test_classify_failure(status, msg, expected):
    assert monitor.classify_failure(status, msg) == expected


def test_every_step_outcome_carries_layer_entity_batch_and_reason(probes):
    probes("mon_ok")
    probes("mon_off", active="N")
    probes("mon_schema", handler="probe_handlers.schema_fail", layer="silver")
    probes("mon_child", deps=["mon_schema"], layer="gold")
    probes("mon_missing", handler="probe_handlers.missing_input")
    meta.set_watermark("mon_ok", 7)
    orchestrator.run(pipelines=["mon_ok", "mon_off", "mon_schema", "mon_child", "mon_missing"], retry_sleep=0)
    rows = {r["pipeline_id"]: r for r in meta.query("SELECT * FROM pipeline_execution_log WHERE pipeline_id LIKE 'mon_%'")}
    assert (rows["mon_ok"]["status"], rows["mon_ok"]["failure_reason"], rows["mon_ok"]["batch_id"]) == ("SUCCESS", None, 7)
    assert (rows["mon_off"]["status"], rows["mon_off"]["failure_reason"]) == ("SKIPPED", "INACTIVE")
    assert (rows["mon_schema"]["status"], rows["mon_schema"]["failure_reason"]) == ("FAILED", "SCHEMA_MISMATCH")
    assert (rows["mon_child"]["status"], rows["mon_child"]["failure_reason"]) == ("BLOCKED", "UPSTREAM_FAILED")
    assert rows["mon_missing"]["failure_reason"] == "MISSING_INPUT"
    assert [rows[p]["layer"] for p in ("mon_ok", "mon_schema", "mon_child")] == ["bronze", "silver", "gold"]
    assert rows["mon_ok"]["entity"] == "ent_mon_ok" and rows["mon_schema"]["batch_id"] is None  # nothing processed yet


def test_alerts_report_failures_and_clear_when_fixed(probes):
    probes("mon_bad", handler="probe_handlers.schema_fail")
    probes("mon_dep", deps=["mon_bad"])
    orchestrator.run(pipelines=["mon_bad", "mon_dep"], retry_sleep=0)
    mine = [a for a in monitor.alerts() if a["subject"] in ("mon_bad", "mon_dep")]
    sev = {a["subject"]: a["severity"] for a in mine}
    assert sev == {"mon_bad": "HIGH", "mon_dep": "MEDIUM"}
    assert "SCHEMA_MISMATCH" in next(a for a in mine if a["subject"] == "mon_bad")["message"]
    order = [a["severity"] for a in monitor.alerts()]
    assert order == sorted(order, key=["HIGH", "MEDIUM", "LOW"].index)   # most severe first
    meta.execute("UPDATE pipeline_metadata SET handler='probe_handlers.ok' WHERE pipeline_id='mon_bad'")
    orchestrator.run(pipelines=["mon_bad", "mon_dep"], retry_sleep=0)
    assert not [a for a in monitor.alerts() if a["subject"] in ("mon_bad", "mon_dep")]


def test_real_run_logs_are_complete(lake):
    """Logs written by the real 23-pipeline run, not by test doubles."""
    rows = meta.query("SELECT l.*, m.target_layer AS ml FROM pipeline_execution_log l JOIN pipeline_metadata m USING(pipeline_id) "
                      "WHERE pipeline_id NOT LIKE 'mon_%' AND pipeline_id NOT LIKE 'probe_%' AND l.status='SUCCESS' AND l.run_id=(SELECT run_id FROM pipeline_execution_log WHERE pipeline_id='gold_customer_360' ORDER BY execution_id LIMIT 1)")
    assert len(rows) == 23
    assert all(r["layer"] == r["ml"] and r["entity"] and r["end_ts"] and r["duration_s"] is not None and r["failure_reason"] is None for r in rows)
    wm = {r["pipeline_id"]: r["last_batch_id"] for r in meta.query("SELECT * FROM pipeline_watermark")}
    assert all(r["batch_id"] == wm.get(r["pipeline_id"]) for r in rows)
    ing = meta.query("SELECT count(*) n, sum(records_loaded) loaded FROM ingestion_audit_log WHERE status='SUCCESS'")[0]
    assert ing["n"] >= 6 and ing["loaded"] > 0
    t = meta.query("SELECT layer, count(*) n FROM transformation_audit_log GROUP BY layer")
    assert {x["layer"] for x in t} >= {"silver_merge", "silver_transform"}


def test_health_summary_shape(lake):
    h = monitor.health_summary()
    assert h["pipelines"] >= 23 and h["status_counts"].get("SUCCESS", 0) >= 23 and h["last_run_id"]
    layers = {f["layer"] for f in h["freshness"]}
    assert {"bronze", "silver_merge", "silver", "gold"} <= layers
    assert set(h["rejected_by_layer"]) >= {"silver", "bronze"} and len(h["slowest"]) == 5
