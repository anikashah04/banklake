"""Silver Transform: deletes propagate, PII is masked only here, valid/rejected split, FK vs 'no children' semantics."""
import duckdb
import pytest

from banklake import meta, orchestrator, reconcile
from banklake.common import LAKE, RAW
from banklake.schemas import ENTITIES

q = lambda sql: duckdb.connect().execute(sql).fetchall()  # noqa: E731


def test_masked_from_bronze_to_silver_and_unmasked_only_in_gold(lake):
    raw_email = q(f"SELECT email FROM read_csv('{RAW}/customer/customer_full.csv', all_varchar=true) WHERE email IS NOT NULL LIMIT 1")[0][0]
    for layer, name in [("bronze", "customer"), ("silver_merge", "customer"), ("silver", "customer")]:
        assert q(f"SELECT count(*) FROM read_parquet('{LAKE}/{layer}/{name}/*.parquet') WHERE email='{raw_email}'")[0][0] == 0, layer
        row = q(f"SELECT email, email_token FROM read_parquet('{LAKE}/{layer}/{name}/*.parquet') WHERE email IS NOT NULL LIMIT 1")[0]
        assert "****@" in row[0] and len(row[1]) == 16, layer
    assert q(f"SELECT count(*) FROM read_parquet('{LAKE}/gold/customer_360/*.parquet') WHERE email='{raw_email}'")[0][0] >= 1
    assert meta.query("SELECT 1 FROM security_audit_log WHERE action='MASK_AT_BRONZE' AND actor='bronze'")


def test_customer_without_loans_is_valid_and_orphans_are_rejected(lake):
    no_loan = q(f"SELECT count(*) FROM read_parquet('{LAKE}/silver/customer/*.parquet') c WHERE NOT EXISTS "
                f"(SELECT 1 FROM read_parquet('{LAKE}/silver/loan/*.parquet') l WHERE l.customer_id = c.customer_id)")[0][0]
    assert no_loan > 0  # most customers have no loan, and they were not rejected
    reasons = dict(q(f"SELECT reject_reason, count(*) FROM read_parquet('{LAKE}/quarantine/silver_loan/*.parquet') GROUP BY 1"))
    assert set(reasons) <= {"ORPHAN_CUSTOMER_ID", "INVALID_LOAN_AMOUNT", "INVALID_INTEREST_RATE", "NULL_LOAN_ID"}
    assert "NO_LOAN" not in reasons


def test_valid_and_rejected_outputs_are_disjoint_and_add_up(lake):
    for e in ENTITIES:
        k = ENTITIES[e]["key"]
        both = q(f"SELECT count(*) FROM read_parquet('{LAKE}/silver/{e}/*.parquet') s JOIN "
                 f"read_parquet('{LAKE}/quarantine/silver_{e}/*.parquet') b ON s.{k} = b.{k}")[0][0]
        assert both == 0, f"{e}: a key is both valid and rejected"
    a = meta.query("SELECT * FROM transformation_audit_log WHERE layer='silver_transform' AND entity='account' ORDER BY id LIMIT 1")[0]
    assert a["rows_out"] + a["rejected"] <= a["rows_in"]  # merge-stage duplicates were already collapsed


@pytest.fixture(autouse=True)
def _six_table_checks(monkeypatch):
    """These tests build the original six-table pipeline; other modules may leave procedure-built layers in the shared lake."""
    monkeypatch.setattr(reconcile, "_lakehouse_built", lambda: False)


def test_source_deletes_remove_rows_from_silver_and_reconcile_stays_clean(lake):
    from banklake import datagen
    info = datagen.generate_incremental(deletes=True)
    st = orchestrator.run(retry_sleep=0)["status"]
    assert set(st.values()) == {"SUCCESS"}, st
    gone = [r[0] for r in q(f"SELECT customer_id FROM read_parquet('{LAKE}/silver_merge/customer/*.parquet') WHERE _is_deleted")]
    assert len(gone) >= info["deleted_customers"]
    in_silver = q(f"SELECT count(*) FROM read_parquet('{LAKE}/silver/customer/*.parquet') WHERE customer_id IN ({','.join('?' * len(gone))})".replace("?", "'%s'") % tuple(gone))[0][0]
    assert in_silver == 0
    assert q(f"SELECT count(*) FROM read_parquet('{LAKE}/silver_merge/customer/*.parquet') WHERE _is_deleted")[0][0] >= 1  # history kept in the merge layer
    t = meta.query("SELECT * FROM transformation_audit_log WHERE layer='silver_transform' AND entity='customer' ORDER BY id DESC LIMIT 1")[0]
    assert t["deletes"] >= 1
    res = reconcile.run_reconciliation()
    key_checks = [r for r in res["results"] if r["check_name"] == "merge_keys_missing_in_silver"]
    assert key_checks and all(r["status"] == "PASS" for r in key_checks), key_checks


def test_changed_record_that_fails_validation_keeps_last_good_version(lake):
    """Last-known-good: an update with a blank name is quarantined; Silver keeps the previous good record."""
    from banklake import datagen
    datagen.generate_incremental()
    target = q(f"SELECT customer_id, city FROM read_parquet('{LAKE}/silver/customer/*.parquet') LIMIT 1")[0]
    f = sorted((RAW / "customer").glob("customer_inc_*.csv"))[-1]
    cols = ",".join(ENTITIES["customer"]["columns"])
    row = {c: "" for c in ENTITIES["customer"]["columns"]}
    row.update(customer_id=target[0], city="Changed City", updated_at="2030-01-01 00:00:00", batch_id="9", op_type="U",
               source_system="CORE_BANKING", created_at="2030-01-01 00:00:00")
    bad = RAW / "customer" / "customer_inc_88.csv"
    bad.write_text(cols + "\n" + ",".join(row[c] for c in ENTITIES["customer"]["columns"]) + "\n")
    try:
        orchestrator.run(layers=["bronze", "silver_merge", "silver"], pipelines=["bronze_customer", "merge_customer", "silver_customer"], retry_sleep=0)
        assert q(f"SELECT city FROM read_parquet('{LAKE}/silver/customer/*.parquet') WHERE customer_id='{target[0]}'")[0][0] == target[1]
        assert q(f"SELECT count(*) FROM read_parquet('{LAKE}/quarantine/silver_customer/*.parquet') WHERE customer_id='{target[0]}' "
                 "AND reject_reason='NULL_CUSTOMER_NAME'")[0][0] >= 1
    finally:
        bad.unlink()
