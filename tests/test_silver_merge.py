"""Silver Merge: CDC rules on small hand-made batches, then an end-to-end run with real generated deletes."""
import duckdb
from pyspark.sql import functions as F

from banklake import meta, orchestrator
from banklake.common import LAKE, RAW, get_spark
from banklake.silver_merge import cdc_processor

COLS = ["customer_id", "customer_name", "city", "op_type", "updated_at", "batch_id", "_batch_id", "_ingest_ts"]


def _df(rows):
    return get_spark().createDataFrame(rows, COLS)


def _actions(df):
    return {r["customer_id"]: r["_merge_action"] for r in df.collect()}


def test_cdc_insert_update_delete_stale_duplicates_and_rejects(lake):
    existing = _df([("C001", "Aarav Shah", "Mumbai", "I", "2026-01-01 00:00:00", "1", 1, "2026-01-01"),
                    ("C002", "Riya Mehta", "Pune", "I", "2026-01-01 00:00:00", "1", 1, "2026-01-01"),
                    ("C004", "Dev Rao", "Delhi", "I", "2026-01-01 00:00:00", "1", 1, "2026-01-01")])
    batch2 = _df([
        ("C002", "Riya Mehta", "Thane", "U", "2026-02-01 00:00:00", "2", 2, "2026-02-01"),   # update
        ("C003", "Kabir Patel", "Pune", "I", "2026-02-01 00:00:00", "2", 2, "2026-02-01"),   # older duplicate ...
        ("C003", "Kabir Patel", "Mumbai", "U", "2026-02-02 00:00:00", "2", 2, "2026-02-01"),  # ... newer wins -> insert
        ("C004", "Dev Rao", "Delhi", "D", "2026-02-01 00:00:00", "2", 2, "2026-02-01"),      # delete
        ("C001", "Aarav Shah", "Goa", "U", "2025-12-01 00:00:00", "2", 2, "2026-02-01"),     # older than target -> stale
        ("C999", "Ghost", "Nowhere", "D", "2026-02-01 00:00:00", "2", 2, "2026-02-01"),      # delete of unknown key
        (None, "No Key", "X", "I", "2026-02-01 00:00:00", "2", 2, "2026-02-01"),             # rejected
        ("C005", "Bad Op", "X", "Z", "2026-02-01 00:00:00", "2", 2, "2026-02-01"),           # rejected
    ])
    latest, bad = cdc_processor.validate_and_dedupe(batch2, "customer_id")
    assert {r["reject_reason"] for r in bad.collect()} == {"NULL_PRIMARY_KEY", "INVALID_OP_TYPE"}
    assert latest.count() == 5  # 6 valid rows, the two C003 versions collapse to one
    classified = cdc_processor.classify(latest, existing, "customer_id")
    assert _actions(classified) == {"C002": "UPDATE", "C003": "INSERT", "C004": "DELETE",
                                    "C001": "STALE", "C999": "DELETE_NO_TARGET"}
    rows = {r["customer_id"]: r for r in cdc_processor.to_target_rows(classified, "run1").collect()}
    assert set(rows) == {"C002", "C003", "C004"}                      # ignored changes are not written
    assert rows["C003"]["city"] == "Mumbai"                            # latest version of the duplicate
    assert rows["C002"]["city"] == "Thane" and rows["C002"]["_is_deleted"] is False
    assert rows["C004"]["_is_deleted"] is True and rows["C004"]["_deleted_ts"] is not None  # soft delete keeps the row


def test_full_load_marks_everything_insert(lake):
    latest, _ = cdc_processor.validate_and_dedupe(
        _df([("C1", "A", "X", "I", "2026-01-01 00:00:00", "1", 1, "t"), ("C2", "B", "Y", None, "2026-01-01 00:00:00", "1", 1, "t")]),
        "customer_id")
    assert set(_actions(cdc_processor.classify(latest, None, "customer_id")).values()) == {"INSERT"}  # null op_type = insert


def test_merge_tables_exist_and_second_run_is_idempotent(lake):
    q = lambda sql: duckdb.connect().execute(sql).fetchall()  # noqa: E731
    for e in ("customer", "account", "transaction", "branch", "deposit", "loan"):
        n = q(f"SELECT count(*), count(DISTINCT {e}_id) FROM read_parquet('{LAKE}/silver_merge/{e}/*.parquet')")[0]
        assert n[0] == n[1], f"{e}: merged table must have one row per key"
    inc = "entity IN ('customer','account','transaction')"  # branch/deposit/loan are full-reload by design
    before = meta.query(f"SELECT count(*) n FROM transformation_audit_log WHERE {inc}")[0]["n"]
    orchestrator.run(layers=["silver_merge"], retry_sleep=0)
    assert meta.query(f"SELECT count(*) n FROM transformation_audit_log WHERE {inc}")[0]["n"] == before  # nothing new -> no work


def test_end_to_end_cdc_batch_with_deletes(lake):
    from banklake import datagen
    q = lambda sql: duckdb.connect().execute(sql).fetchall()  # noqa: E731
    path = f"{LAKE}/silver_merge/customer/*.parquet"
    base_rows = q(f"SELECT count(*) FROM read_parquet('{path}')")[0][0]
    info = datagen.generate_incremental(deletes=True)
    st = orchestrator.run(layers=["bronze", "silver_merge"], retry_sleep=0)["status"]
    assert set(st.values()) == {"SUCCESS"}, st
    a = meta.query("SELECT * FROM transformation_audit_log WHERE entity='customer' AND layer='silver_merge' ORDER BY id DESC LIMIT 1")[0]
    assert a["inserts"] == info["new_customers"] and a["deletes"] == info["deleted_customers"]  # a null *name* is still a valid key here
    # an update whose random timestamp is older than the version already merged is correctly ignored as STALE
    assert a["updates"] + a["ignored"] == info["updated_customers"]
    assert q(f"SELECT count(*) FROM read_parquet('{path}')")[0][0] == base_rows + a["inserts"]   # deletes keep the row
    assert q(f"SELECT count(*) FROM read_parquet('{path}') WHERE _is_deleted")[0][0] == info["deleted_customers"]
    t = meta.query("SELECT * FROM transformation_audit_log WHERE entity='transaction' AND layer='silver_merge' ORDER BY id DESC LIMIT 1")[0]
    assert t["deletes"] == info["deleted_transactions"]
