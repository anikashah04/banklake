"""Four-stage validation and discrepancy tracing: clean data passes, and damage at each layer is caught at the
FIRST stage where it appears, with the exact missing keys and the reason."""
import shutil
from contextlib import contextmanager

import duckdb
import pytest

from banklake import orchestrator, reconcile, trace
from banklake.common import LAKE
from banklake.schemas import ORDER


@pytest.fixture(scope="module")
def synced(lake):
    """Earlier test modules leave generated batches half-processed and a deleted raw file behind; rebuild a consistent state."""
    assert set(orchestrator.reload(["bronze_customer"], retry_sleep=0)["status"].values()) == {"SUCCESS"}
    assert set(orchestrator.run(retry_sleep=0)["status"].values()) == {"SUCCESS"}


@contextmanager
def damaged(layer, name, select_sql):
    """Temporarily rewrite a lake table with `select_sql` ({src} = the table) and restore it afterwards."""
    d = LAKE / layer / name
    bak, tmp = str(d) + "__bak", str(d) + "__tmp"
    shutil.copytree(d, bak)
    shutil.rmtree(tmp, ignore_errors=True)
    (d.parent / (name + "__tmp")).mkdir()
    duckdb.connect().execute(f"COPY ({select_sql.format(src=f"read_parquet('{bak}/*.parquet')")}) TO '{tmp}/part-damaged.parquet' (FORMAT PARQUET)")
    shutil.rmtree(d)
    shutil.move(tmp, d)
    try:
        yield
    finally:
        shutil.rmtree(d)
        shutil.move(bak, d)


def _fails(res):
    return {(r["entity"], r["check_name"]) for r in res["results"] if r["status"] == "FAIL"}


def test_clean_lake_passes_all_four_stages(synced):
    res = reconcile.run_reconciliation()
    assert _fails(res) == set(), _fails(res)
    stages = {r["stage"] for r in res["results"]}
    assert stages == {"BRONZE", "SILVER_MERGE", "SILVER_TRANSFORM", "GOLD"}
    names = {r["check_name"] for r in res["results"]}
    for expected in ["raw_rows_vs_bronze_plus_corrupt", "file_tracker_loaded_vs_bronze_rows", "bronze_keys_missing_in_merge",
                     "merge_duplicate_keys", "delete_flag_vs_latest_cdc_operation", "merge_keys_missing_in_silver",
                     "deleted_keys_still_in_silver", "orphan_customer_id_in_silver", "orphan_account_id_in_silver", "sum_balance",
                     "unexplained_missing_records", "gold_customer_360_rows_vs_silver", "gold_loan_amount_vs_silver",
                     "gold_deposit_amount_vs_silver", "gold_txn_count_vs_silver", "gold_customer_balance_vs_silver"]:
        assert expected in names, expected


def test_unmatched_parents_are_information_not_errors(synced):
    res = reconcile.run_reconciliation()
    info = {r["check_name"]: r for r in res["results"] if r["check_name"].startswith("info_customers_without_")}
    assert set(info) == {"info_customers_without_accounts", "info_customers_without_deposits", "info_customers_without_loans"}
    assert int(info["info_customers_without_loans"]["target_value"]) > 0 and info["info_customers_without_loans"]["status"] == "PASS"


@pytest.mark.parametrize("entity", ORDER)
def test_every_missing_source_record_is_explained_on_a_clean_lake(synced, entity):
    t = trace.trace(entity)
    assert t["unexplained_total"] == 0 and t["extra_in_silver"] == []
    c = t["layer_counts"]
    assert c["raw_distinct_keys"] >= c["silver_transform_keys"]
    assert t["missing_total"] == t["explained_total"]


def test_trace_reports_reasons_and_cdc_deletes(synced):
    from banklake import datagen
    datagen.generate_incremental(deletes=True)  # a CDC batch with deletes, processed through every layer
    assert set(orchestrator.run(retry_sleep=0)["status"].values()) == {"SUCCESS"}
    assert _fails(reconcile.run_reconciliation()) == set()
    t = trace.trace("account")
    assert any(r.startswith("rejected in Silver Transform: ORPHAN_CUSTOMER_ID") for r in t["by_reason"])
    cust = trace.trace("customer")
    assert any("soft-deleted by the source" in r for r in cust["by_reason"])
    assert cust["layer_counts"]["silver_merge_soft_deleted"] > 0
    assert "UNEXPLAINED 0" in trace.format_trace(cust)


def test_spec_scenario_records_missing_after_transform(synced):
    """Source has N accounts, the transformed output has fewer: find the ids, the layer and the reason."""
    n = 40
    before = trace.trace("account")["layer_counts"]["silver_transform_keys"]
    with damaged("silver", "account", f"SELECT * FROM {{src}} WHERE account_id NOT IN (SELECT account_id FROM {{src}} ORDER BY account_id LIMIT {n})"):
        t = trace.trace("account", limit=100)
        assert t["layer_counts"]["silver_transform_keys"] == before - n
        assert t["unexplained_total"] == n
        assert {u["first_missing_layer"] for u in t["unexplained"]} == {"silver_transform"}
        assert all("lost in Silver Transform" in u["reason"] for u in t["unexplained"])
        res = reconcile.run_reconciliation()
        assert res["first_failing_stage"]["stage"] == "SILVER_TRANSFORM"
        assert {("account", "merge_keys_missing_in_silver"), ("account", "unexplained_missing_records")} <= _fails(res)
    assert _fails(reconcile.run_reconciliation()) == set()


def test_damage_in_silver_merge_is_caught_at_the_merge_stage(synced):
    with damaged("silver_merge", "account", "SELECT * FROM {src} WHERE account_id NOT IN (SELECT account_id FROM {src} ORDER BY account_id LIMIT 12)"):
        res = reconcile.run_reconciliation()
        assert res["first_failing_stage"]["stage"] == "SILVER_MERGE"
        assert ("account", "bronze_keys_missing_in_merge") in _fails(res)


def test_wrong_delete_flag_is_a_column_level_mismatch(synced):
    with damaged("silver_merge", "customer", "SELECT * REPLACE (CASE WHEN customer_id IN (SELECT customer_id FROM {src} ORDER BY customer_id LIMIT 6) THEN true ELSE _is_deleted END AS _is_deleted) FROM {src}"):
        res = reconcile.run_reconciliation()
        assert ("customer", "delete_flag_vs_latest_cdc_operation") in _fails(res)
        assert res["first_failing_stage"]["stage"] == "SILVER_MERGE"


def test_rows_missing_from_bronze_are_caught_at_the_first_stage(synced):
    with damaged("bronze", "loan", "SELECT * FROM {src} WHERE loan_id NOT IN (SELECT loan_id FROM {src} ORDER BY loan_id LIMIT 9)"):
        res = reconcile.run_reconciliation()
        assert res["first_failing_stage"]["stage"] == "BRONZE"
        assert ("loan", "raw_rows_vs_bronze_plus_corrupt") in _fails(res)
        t = trace.trace("loan")
        # a removed key that Silver had also rejected earlier is now only traceable to the layer that lost it
        assert {u["first_missing_layer"] for u in t["unexplained"]} <= {"bronze"}


def test_gold_duplicates_and_wrong_totals_are_caught_at_gold(synced):
    with damaged("gold", "customer_360", "SELECT * FROM {src} UNION ALL (SELECT * FROM {src} LIMIT 5)"):
        res = reconcile.run_reconciliation()
        assert res["first_failing_stage"]["stage"] == "GOLD"
        assert {("customer", "gold_customer_360_duplicate_keys"), ("customer", "gold_customer_360_rows_vs_silver")} <= _fails(res)
    with damaged("gold", "loan_summary", "SELECT * REPLACE (total_loan_amount + 1 AS total_loan_amount) FROM {src}"):
        assert ("loan", "gold_loan_amount_vs_silver") in _fails(reconcile.run_reconciliation())
    assert _fails(reconcile.run_reconciliation()) == set()
