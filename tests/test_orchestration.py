"""Metadata-driven orchestration: the tables, not the code, decide what runs, in which order, and how often."""
import sqlite3

import pytest

import probe_handlers
from banklake import meta, orchestrator


@pytest.fixture()
def probes():
    """Create probe pipelines in the metadata DB, remove them (and their logs) afterwards."""
    made = []

    def add(pid, handler="probe_handlers.ok", order=1, retries=2, active="Y", deps=()):
        meta.execute("INSERT INTO pipeline_metadata(pipeline_id,source_table,target_table,source_layer,target_layer,load_strategy,"
                     "is_active,handler,max_retries,execution_order) VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (pid, "x", "x", "raw", "bronze", "full", active, handler, retries, order))
        for d in deps:
            meta.execute("INSERT INTO pipeline_dependencies VALUES(?,?)", (pid, d))
        made.append(pid)

    meta.init_db()
    probe_handlers.CALLS.clear()
    probe_handlers._attempts.clear()
    yield add
    for pid in made:
        for t in ("pipeline_metadata", "pipeline_execution_log", "pipeline_error_log"):
            meta.execute(f"DELETE FROM {t} WHERE pipeline_id=?", (pid,))
        meta.execute("DELETE FROM pipeline_dependencies WHERE pipeline_id=? OR depends_on=?", (pid, pid))


def test_pipeline_defined_only_in_metadata_runs_in_execution_order(probes):
    probes("probe_c", order=3)
    probes("probe_a", order=1)
    probes("probe_b", order=2)
    orchestrator.run(pipelines=["probe_a", "probe_b", "probe_c"], retry_sleep=0)
    assert probe_handlers.CALLS == ["probe_a", "probe_b", "probe_c"]
    meta.execute("UPDATE pipeline_metadata SET execution_order=0 WHERE pipeline_id='probe_c'")  # change metadata only
    probe_handlers.CALLS.clear()
    orchestrator.run(pipelines=["probe_a", "probe_b", "probe_c"], retry_sleep=0)
    assert probe_handlers.CALLS == ["probe_c", "probe_a", "probe_b"]


def test_dependencies_beat_execution_order(probes):
    probes("probe_first", order=9)
    probes("probe_second", order=1, deps=["probe_first"])
    orchestrator.run(pipelines=["probe_second", "probe_first"], retry_sleep=0)
    assert probe_handlers.CALLS == ["probe_first", "probe_second"]


def test_inactive_skipped_failed_blocks_downstream_but_not_independent(probes):
    probes("probe_off", active="N")
    probes("probe_bad", handler="probe_handlers.always_fail", retries=1)
    probes("probe_child", deps=["probe_bad"])
    probes("probe_free", order=5)
    st = orchestrator.run(pipelines=["probe_off", "probe_bad", "probe_child", "probe_free"], retry_sleep=0)["status"]
    assert st == {"probe_off": "SKIPPED", "probe_bad": "FAILED", "probe_child": "BLOCKED", "probe_free": "SUCCESS"}
    assert "probe_child" not in probe_handlers.CALLS and "probe_off" not in probe_handlers.CALLS


def test_retry_count_comes_from_metadata(probes):
    probes("probe_flaky", handler="probe_handlers.fail_once", retries=2)
    probes("probe_noretry", handler="probe_handlers.always_fail", retries=0)
    probes("probe_twice", handler="probe_handlers.always_fail", retries=3)
    st = orchestrator.run(pipelines=["probe_flaky", "probe_noretry", "probe_twice"], retry_sleep=0)["status"]
    assert st["probe_flaky"] == "SUCCESS" and st["probe_noretry"] == "FAILED" and st["probe_twice"] == "FAILED"
    tries = {r["pipeline_id"]: r["retry_count"] for r in meta.query("SELECT pipeline_id, retry_count FROM pipeline_execution_log WHERE pipeline_id LIKE 'probe_%'")}
    assert tries == {"probe_flaky": 1, "probe_noretry": 0, "probe_twice": 3}
    assert probe_handlers.CALLS.count("probe_twice") == 4  # 1 try + 3 retries


def test_every_pipeline_has_complete_metadata(lake):
    rows = meta.query("SELECT * FROM pipeline_metadata WHERE pipeline_id NOT LIKE 'probe_%'")
    assert len(rows) == 23
    for r in rows:
        assert r["source_database"] and r["target_database"] and r["execution_order"] is not None and r["handler"]
        assert r["max_retries"] is not None and r["is_active"] in ("Y", "N")
        if r["target_layer"] in ("silver_merge", "silver"):
            assert r["watermark_column"] == "_batch_id" and r["primary_key"]
    orders = [r["execution_order"] for r in rows]
    assert len(set(orders)) == len(orders)  # a total order, no ties


def test_watermark_moves_on_success_and_rerun_does_no_work(lake):
    bronze_max = None
    import duckdb
    from banklake.common import LAKE
    bronze_max = duckdb.connect().execute(f"SELECT max(_batch_id) FROM read_parquet('{LAKE}/bronze/customer/*.parquet')").fetchone()[0]
    w = meta.query("SELECT * FROM pipeline_watermark WHERE pipeline_id='merge_customer'")[0]
    assert w["last_batch_id"] == bronze_max and w["last_run_id"] and w["updated_ts"]
    orchestrator.run(pipelines=["bronze_customer", "merge_customer", "silver_customer"], retry_sleep=0)
    last = {r["pipeline_id"]: r for r in meta.query("SELECT * FROM pipeline_execution_log ORDER BY execution_id DESC LIMIT 3")}
    assert last["merge_customer"]["rows_in"] == 0 and last["bronze_customer"]["rows_in"] == 0


def test_failed_run_does_not_move_the_watermark(lake):
    before = meta.get_watermark("merge_customer")
    meta.execute("UPDATE pipeline_metadata SET handler='banklake.silver_merge.loader.nope' WHERE pipeline_id='merge_customer'")
    try:
        st = orchestrator.run(pipelines=["merge_customer"], retry_sleep=0)["status"]
    finally:
        meta.execute("UPDATE pipeline_metadata SET handler='banklake.silver_merge.loader.run' WHERE pipeline_id='merge_customer'")
        orchestrator.run(pipelines=["merge_customer"], retry_sleep=0)  # a failed upstream blocks others until it succeeds again
    assert st["merge_customer"] == "FAILED" and meta.get_watermark("merge_customer") == before


def test_downstream_closure():
    d = set(orchestrator.downstream_of(["bronze_loan"]))
    assert {"bronze_loan", "merge_loan", "silver_loan", "gold_loan_summary", "gold_customer_360"} <= d
    assert "silver_transaction" not in d and "bronze_customer" not in d


def test_reload_rebuilds_pipeline_and_downstream(lake):
    import duckdb
    from banklake.common import LAKE
    n = lambda: duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{LAKE}/silver/customer/*.parquet')").fetchone()[0]  # noqa: E731
    before = n()
    res = orchestrator.reload(["silver_customer"], retry_sleep=0)
    assert set(res["status"].values()) == {"SUCCESS"}
    assert {"silver_customer", "silver_account", "silver_transaction", "gold_customer_360"} <= set(res["status"])
    assert n() == before
    types = {r["run_type"] for r in meta.query("SELECT run_type FROM pipeline_execution_log WHERE run_id=?", (res["run_id"],))}
    assert types == {"RELOAD"}
    only = orchestrator.reload(["silver_customer"], downstream=False, retry_sleep=0)
    assert list(only["status"]) == ["silver_customer"]
    with pytest.raises(ValueError):
        orchestrator.reload(["no_such_pipeline"])


def test_old_metadata_database_is_upgraded_without_losing_edits(tmp_path, monkeypatch):
    db = tmp_path / "old.db"
    c = sqlite3.connect(db)
    c.executescript("""CREATE TABLE pipeline_metadata(pipeline_id TEXT PRIMARY KEY, source_table TEXT, target_table TEXT,
        source_layer TEXT, target_layer TEXT, load_strategy TEXT, primary_key TEXT, is_active TEXT DEFAULT 'Y', handler TEXT,
        max_retries INTEGER DEFAULT 2);
        CREATE TABLE pipeline_watermark(pipeline_id TEXT PRIMARY KEY, last_batch_id INTEGER);
        CREATE TABLE pipeline_execution_log(execution_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, pipeline_id TEXT);""")
    c.execute("INSERT INTO pipeline_metadata VALUES('silver_loan','loan','loan','bronze','silver','full','loan_id','N','banklake.silver.run_loan',5)")
    c.commit()
    c.close()
    monkeypatch.setattr(meta, "DB_PATH", db)
    meta.init_db()
    meta.init_db()  # idempotent
    r = meta.query("SELECT * FROM pipeline_metadata WHERE pipeline_id='silver_loan'")[0]
    assert r["is_active"] == "N" and r["max_retries"] == 5                       # user edits kept
    assert r["handler"] == "banklake.silver_transform.loader.run_loan"          # old handler migrated
    assert r["execution_order"] == 35 and r["target_database"] == "banklake_silver" and r["watermark_column"] == "_batch_id"
    assert "run_type" in [x["name"] for x in meta.query("PRAGMA table_info(pipeline_execution_log)")]
