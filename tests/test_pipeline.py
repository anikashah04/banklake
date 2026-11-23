import re

import pytest

from conftest import quarantine_counts, silver


def test_injected_anomalies_are_quarantined(lake):
    """Every injected primary anomaly is caught (cascade rejects may add to ORPHAN counts)."""
    for entity, reasons in lake.items():
        got = quarantine_counts(entity) if entity != "branch" else {}
        for reason, n in reasons.items():
            if reason.isupper() and not reason.startswith("ORPHAN"):
                assert got.get(reason) == n, (entity, reason, got)
            elif reason.startswith("ORPHAN"):
                assert got.get(reason, 0) >= n, (entity, reason, got)


def test_silver_is_deduplicated_and_typed(lake):
    c, t = silver("customer"), silver("transaction")
    assert c["customer_id"].is_unique and t["transaction_id"].is_unique
    assert (t["amount"] > 0).all()
    assert c["segment"].isin(["RETAIL", "PREMIUM", "SME", "CORPORATE"]).all()  # messy ' retail ' fixed


def test_gold_is_the_unmasked_layer(lake):
    """Masked at Bronze, Silver Merge and Silver Transform; Gold customer_360 is unmasked from the vault (audited)."""
    import duckdb
    from banklake import meta
    from banklake.common import LAKE
    df = duckdb.connect().execute(f"SELECT * FROM read_parquet('{LAKE}/gold/customer_360/*.parquet')").fetchdf()
    assert not df["email"].str.contains(r"\*{4}", regex=True).any()
    assert df["email"].str.contains("@").all()
    assert not df["phone"].str.contains(r"\*", regex=True).any()
    assert not df["customer_name"].str.contains(r"\*", regex=True).any()
    assert df["customer_token"].str.len().eq(16).all()
    assert meta.query("SELECT 1 FROM security_audit_log WHERE action='UNMASK_AT_GOLD' AND actor='gold_pipeline'")


def test_reconciliation_clean_run_has_no_failures(lake):
    from banklake import reconcile
    res = reconcile.run_reconciliation()
    assert [r for r in res["results"] if r["status"] == "FAIL"] == []
    assert res["first_failing_stage"] is None


def test_incremental_merge_and_idempotency(lake):
    import pandas as pd
    from banklake import datagen, meta, orchestrator
    from banklake.common import RAW
    before = len(silver("customer"))
    info = datagen.generate_incremental()
    r = orchestrator.run(retry_sleep=0)
    assert set(r["status"].values()) == {"SUCCESS"}
    after = silver("customer")
    assert len(after) == before + info["new_customers"] - 1  # one injected null-name new customer
    inc = pd.read_csv(RAW / "customer" / "customer_inc_1.csv", dtype=str)
    changed = inc[~inc["customer_id"].isin([f"C{i:06d}" for i in range(1001, 1031)])].dropna(subset=["customer_name"])
    merged = after.set_index("customer_id").loc[changed["customer_id"]]
    assert (merged["city"].str.lower().values == changed["city"].str.strip().str.lower().values).all()  # UPDATE applied

    # second run: nothing new -> nothing processed, nothing duplicated
    n_acc, n_txn = len(silver("account")), len(silver("transaction"))
    orchestrator.run(retry_sleep=0)
    last = {x["pipeline_id"]: x for x in meta.query("SELECT * FROM pipeline_execution_log ORDER BY execution_id DESC LIMIT (SELECT count(*) FROM pipeline_metadata)")}
    assert last["silver_customer"]["rows_in"] == 0 and last["bronze_transaction"]["rows_in"] == 0
    assert (len(silver("customer")), len(silver("account")), len(silver("transaction"))) == (len(after), n_acc, n_txn)


@pytest.mark.parametrize("mode, failing_check", [
    ("delete", "merge_keys_missing_in_silver"),
    ("duplicate", "duplicate_keys"),
    ("alter", "row_level_balance_mismatch"),
])
def test_injected_discrepancy_is_detected_then_repaired(lake, mode, failing_check):
    from banklake import orchestrator, reconcile
    reconcile_ok = reconcile.run_reconciliation()
    assert reconcile_ok["first_failing_stage"] is None
    reconcile.inject_discrepancy("account", mode, 7)
    res = reconcile.run_reconciliation()
    fails = {(r["entity"], r["check_name"]) for r in res["results"] if r["status"] == "FAIL"}
    assert ("account", failing_check) in fails, fails
    assert res["first_failing_stage"]["stage"] == "SILVER_TRANSFORM"
    # repair: full refresh of Silver account from Bronze history
    orchestrator.run(pipelines=["silver_account"], full_refresh=True, retry_sleep=0)
    assert reconcile.run_reconciliation()["first_failing_stage"] is None


def test_failed_pipeline_blocks_downstream_and_inactive_is_skipped(lake):
    from banklake import meta, orchestrator
    meta.execute("UPDATE pipeline_metadata SET handler='banklake.bronze.does_not_exist' WHERE pipeline_id='bronze_loan'")
    meta.execute("UPDATE pipeline_metadata SET is_active='N' WHERE pipeline_id='bronze_branch'")
    try:
        st = orchestrator.run(pipelines=["bronze_loan", "silver_loan", "gold_loan_summary", "bronze_branch"], retry_sleep=0)["status"]
    finally:
        meta.execute("UPDATE pipeline_metadata SET handler='banklake.bronze.run' WHERE pipeline_id='bronze_loan'")
        meta.execute("UPDATE pipeline_metadata SET is_active='Y' WHERE pipeline_id='bronze_branch'")
    assert st["bronze_loan"] == "FAILED"
    assert st["silver_loan"] == "BLOCKED" and st["gold_loan_summary"] == "BLOCKED"
    assert st["bronze_branch"] == "SKIPPED"
    row = meta.query("SELECT retry_count FROM pipeline_execution_log WHERE pipeline_id='bronze_loan' ORDER BY execution_id DESC LIMIT 1")[0]
    assert row["retry_count"] == 2  # 1 try + 2 retries
    assert meta.query("SELECT 1 FROM pipeline_error_log WHERE pipeline_id='bronze_loan'")


def test_bad_source_schema_fails_with_clear_message(lake):
    from banklake import meta, orchestrator
    from banklake.common import RAW
    bad = RAW / "loan" / "loan_inc_9.csv"
    bad.write_text("loan_id,customer_id\nL1,C000001\n")
    try:
        st = orchestrator.run(pipelines=["bronze_loan"], retry_sleep=0)["status"]
    finally:
        bad.unlink()
        meta.execute("DELETE FROM ingested_files WHERE file_name='loan_inc_9.csv'")
    assert st["bronze_loan"] == "FAILED"
    err = meta.query("SELECT error_message FROM pipeline_execution_log WHERE pipeline_id='bronze_loan' ORDER BY execution_id DESC LIMIT 1")[0]
    assert "Schema mismatch" in err["error_message"] and "loan_amount" in err["error_message"]


def test_masking_python_and_spark_agree(lake):
    from banklake.common import get_spark
    from banklake.masking import mask_col, mask_value
    from pyspark.sql import functions as F
    assert mask_value("NAME", "Aarav Mehta") == "A**** M****"
    assert mask_value("EMAIL", "aarav@example.com") == "a****@example.com"
    assert mask_value("ACCOUNT", "123456789012") == "******9012"
    assert mask_value("PHONE", "9876543210") == "******3210"
    sp = get_spark()
    for rule, v in [("NAME", "Aarav Mehta"), ("EMAIL", "aarav@example.com"), ("ACCOUNT", "123456789012"), ("PHONE", "9876543210")]:
        got = sp.createDataFrame([(v,)], ["x"]).select(mask_col(rule, F.col("x"))).first()[0]
        assert got == mask_value(rule, v)


def test_no_clear_pii_anywhere_in_lake(lake):
    """Raw emails/phones/names/account numbers must not appear in Bronze, Silver Merge or Silver (Gold is unmasked by design)."""
    from banklake import vault
    for ent, col in [("customer", "email"), ("customer", "phone"), ("customer", "customer_name"), ("account", "account_number")]:
        scan = vault.leak_scan(ent, col)
        assert scan["_checked_values"] > 100
        assert {k: v for k, v in scan.items() if not k.startswith("_")} == {k: 0 for k in scan if not k.startswith("_")}, (ent, col, scan)
        assert {"bronze", "silver_merge", "silver"} <= set(scan) and "gold" not in scan


def test_leak_scan_can_actually_fail(lake):
    """Guard against a scan that always passes: put clear emails into a copy of Silver and expect a hit."""
    import shutil
    import duckdb
    import pandas as pd
    from banklake import vault
    from banklake.common import LAKE, RAW
    sdir = LAKE / "silver" / "customer"
    backup = str(sdir) + "__bak"
    shutil.copytree(sdir, backup)
    try:
        raw = pd.read_csv(RAW / "customer" / "customer_full.csv", dtype=str)["email"].dropna().iloc[0]
        con = duckdb.connect()
        con.execute(f"COPY (SELECT * REPLACE ('{raw}' AS email) FROM read_parquet('{sdir}/*.parquet') LIMIT 3) TO '{sdir}/leak.parquet' (FORMAT PARQUET)")
        assert vault.leak_scan("customer", "email")["silver"] >= 3
    finally:
        shutil.rmtree(sdir)
        shutil.move(backup, sdir)
    assert vault.leak_scan("customer", "email")["silver"] == 0


def test_vault_roundtrip_roles_and_audit(lake):
    import duckdb
    import pandas as pd
    from banklake import meta, vault
    from banklake.common import LAKE, RAW
    row = duckdb.connect().execute(f"SELECT customer_id, customer_name_token, email_token FROM read_parquet('{LAKE}/silver/customer/*.parquet') ORDER BY customer_id LIMIT 1").fetchall()[0]
    raw = pd.read_csv(RAW / "customer" / "customer_full.csv", dtype=str).set_index("customer_id")
    # allowed role gets the real value back
    got = vault.reveal("relationship_manager", "customer", "email", [row[2]])
    assert got[row[2]] == raw.loc[row[0], "email"]
    assert vault.reveal("compliance_officer", "customer", "customer_name", [row[1]])[row[1]].lower() == raw.loc[row[0], "customer_name"].lower()
    # roles without permission are refused and the refusal is audited
    for r in ("analyst", "executive"):
        with pytest.raises(PermissionError):
            vault.reveal(r, "customer", "email", [row[2]])
    # branch managers may see names but not phones
    assert vault.can_unmask("branch_manager", "customer", "customer_name")
    assert not vault.can_unmask("branch_manager", "customer", "phone")
    actions = {(x["actor"], x["action"]) for x in meta.query("SELECT actor, action FROM security_audit_log")}
    assert ("relationship_manager", "UNMASK") in actions and ("analyst", "UNMASK_DENIED") in actions


def test_reveal_df_masks_or_reveals_by_role(lake):
    import duckdb
    from banklake import vault
    from banklake.common import LAKE
    df = duckdb.connect().execute(f"SELECT customer_id, customer_name, email, phone, customer_name_token, email_token, phone_token FROM read_parquet('{LAKE}/silver/customer/*.parquet') LIMIT 20").fetchdf()
    a, rev, masked = vault.reveal_df(df, "customer", "analyst")
    assert rev == [] and set(masked) == {"customer_name", "email", "phone"} and a["email"].str.contains(r"\*{4}").all()
    r, rev, masked = vault.reveal_df(df, "customer", "relationship_manager")
    assert set(rev) == {"customer_name", "email", "phone"} and not r["email"].str.contains(r"\*").any()


def test_token_python_matches_spark(lake):
    from banklake import vault
    from banklake.common import get_spark
    from banklake.masking import tokenize_expr
    from pyspark.sql import functions as F
    sp = get_spark()
    for v in ["aarav@example.com", "9876543210", "Zoë Müller"]:
        assert sp.createDataFrame([(v,)], ["x"]).select(tokenize_expr(F.col("x"))).first()[0] == vault.token_for(v)


def test_gold_tables_support_bi_questions(lake):
    """The dashboards read only Gold: check the columns they depend on exist and reconcile with Silver."""
    import duckdb
    from banklake.common import LAKE
    c = duckdb.connect()
    g = lambda t, cols: c.execute(f"SELECT {cols} FROM read_parquet('{LAKE}/gold/{t}/*.parquet')").fetchdf()  # noqa: E731
    assert {"maturity_month", "total_deposits", "n_deposits", "segment"} <= set(g("deposit_summary", "*").columns)
    assert {"customer_name_token", "total_balance", "total_deposits", "total_loans"} <= set(g("customer_360", "*").columns)
    # loan book in Gold equals loans in Silver whose customer exists
    gold_loans = g("loan_summary", "sum(total_loan_amount) s").s[0]
    silver_loans = c.execute(f"SELECT sum(loan_amount) FROM read_parquet('{LAKE}/silver/loan/*.parquet')").fetchall()[0][0]
    assert abs(float(gold_loans) - float(silver_loans)) < 0.005
    # FD totals reconcile too
    fd_silver = c.execute(f"SELECT sum(deposit_amount) FROM read_parquet('{LAKE}/silver/deposit/*.parquet')").fetchall()[0][0]
    assert abs(float(g("deposit_summary", "sum(total_deposits) s").s[0]) - float(fd_silver)) < 0.005


def test_sql_converter_rules_and_refusals():
    from banklake.sqlmigrate import convert
    c = convert("SEL ZEROIFNULL(SUM(x)) FROM legacy_db.account WHERE d >= ADD_MONTHS(DATE '2026-06-30', -12);")
    assert "SELECT COALESCE(SUM(x), 0)" in c.converted
    assert "date_add('month', -12, DATE '2026-06-30')" in c.converted
    assert "banklake.silver_account" in c.converted and c.status == "CONVERTED"
    assert "LIMIT 5" in convert("SELECT TOP 5 a FROM legacy_db.loan ORDER BY a").converted
    u = convert("SEL a FROM legacy_db.account QUALIFY ROW_NUMBER() OVER (ORDER BY a) = 1")
    assert u.status == "UNSUPPORTED" and u.unsupported == ["QUALIFY clause"]


def test_sql_migration_results_match_pyspark(lake):
    from banklake import sqlmigrate
    res = {r["query"]: r for r in sqlmigrate.run_all()}
    assert res["legacy_branch_segment_summary"]["match"] is True
    assert res["legacy_recent_accounts"]["match"] is True
    assert res["legacy_unsupported"]["match"] is None


def test_topological_order_and_cycle_detection():
    from banklake.orchestrator import topo_sort
    assert topo_sort(["c", "b", "a"], {"c": ["b"], "b": ["a"]}) == ["a", "b", "c"]
    with pytest.raises(ValueError):
        topo_sort(["a", "b"], {"a": ["b"], "b": ["a"]})
