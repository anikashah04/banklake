"""Procedure-driven transformation: Teradata SELECT -> Athena SELECT -> EMR PySpark script -> target tables, with the layer audit log."""
import re
import time

import duckdb
import pytest

from banklake import meta
from banklake.common import LAKE
from banklake.transform import catalog, convert, engine

RD = "2026-11-25"


def q(sql):
    return duckdb.connect().execute(sql).fetchall()


def pq(layer, table):
    return f"read_parquet('{engine.runtime.table_path(layer, table)}/*.parquet')"


def test_catalog_has_three_layers_named_by_target():
    c = {l: catalog.procs_for(l) for l in catalog.LAYERS}
    assert len(c["silver_merge"]) == 23 and len(c["silver_transform"]) == 52 and len(c["gold"]) == 14
    assert {p["source_db"] for p in c["silver_merge"]} == {"bronze"}
    assert {p["source_db"] for p in c["silver_transform"]} == {"silver_merge"} and {p["source_db"] for p in c["gold"]} == {"silver_transform"}
    assert all(p["target"] in p["proc_id"] and p["teradata"].lstrip().upper().startswith(("SEL", "WITH")) for p in catalog.PROCS)
    from banklake.schemas import ENTITIES, SOURCE_ONLY
    for e in list(ENTITIES) + list(SOURCE_ONLY):
        for pid in (f"silver_merge.{e}_merge", f"silver_transform.{e}_clean", f"silver_transform.{e}_rejects"):
            assert pid in catalog.BY_ID


def test_upstream_resolution():
    ids = [p["proc_id"] for p in catalog.upstream("gold.gold_customer_overview")]
    assert ids == ["silver_merge.customer_merge", "silver_merge.account_merge", "silver_transform.customer_account_summary"]
    assert catalog.upstream("silver_merge.customer_merge") == []


@pytest.mark.parametrize("proc", catalog.PROCS, ids=lambda p: p["proc_id"])
def test_every_procedure_converts_and_is_select_only(proc):
    assert re.match(r"^\s*(SEL|WITH)\b", proc["teradata"])
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE)\b", proc["teradata"], re.I)
    g = convert.generate(proc)
    assert g["status"] == "CONVERTED", g["problems"]
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE)\b", g["athena"], re.I)
    assert re.match(r"^\s*(SELECT|WITH)\b", g["athena"])
    assert "SEL " not in g["athena"] and "ZEROIFNULL" not in g["athena"].upper()
    assert "DATE('{args.report_date}')" in g["athena"] or "report_date" in g["athena"]
    # schema dots -> underscores, so every source is read as <database>_<table>
    for s in proc["sources"]:
        assert f"{proc['source_db']}_{s}" in g["athena"]
        assert f"{proc['source_db']}.{s}" not in g["athena"]
    assert "{report_date}" not in g["athena"] and "{report_date}" not in g["emr_sql"]


def test_teradata_to_athena_rules_are_applied():
    g = convert.generate(catalog.BY_ID["gold.gold_deposit_summary"])
    assert "date_format(d.maturity_date, '%Y-%m')" in g["athena"]          # TO_CHAR -> date_format
    assert "DATE_FORMAT(d.maturity_date, 'yyyy-MM')" in g["emr_sql"]       # and to Spark
    assert "EXTRACT(YEAR FROM d.maturity_date)" in g["athena"]             # not mistaken for a table (converter bug fixed)
    g = convert.generate(catalog.BY_ID["gold.gold_customer_360"])
    assert "COALESCE(" in g["athena"] and "ZEROIFNULL" not in g["athena"]
    g = convert.generate(catalog.BY_ID["silver_transform.account_clean"])
    assert "TRY_CAST(" in g["athena"] and "DATE('{args.report_date}')" in g["athena"]


def test_generated_script_follows_the_argument_and_iceberg_pattern():
    p = catalog.BY_ID["silver_transform.customer_account_summary"]
    s = convert.generate(p)["script"]
    compile(s, "x.py", "exec")
    for a in ("catalog", "source_db", "target_tables", "report_date", "secret_name", "env", "region", "script_name"):
        assert f'"--" + name' in s and f"'{a}'" in s
    for needle in ("org.apache.iceberg.spark.SparkCatalog", "org.apache.iceberg.aws.glue.GlueCatalog", "org.apache.iceberg.aws.s3.S3FileIO",
                   "s3://id-aws-{args.env}-s3-gold-pii/data/warehouse/", "DELETE FROM {full} WHERE report_date = DATE('{args.report_date}')",
                   "df.writeTo(full).append()", "createOrReplaceTempView", "silver_merge_account"):
        assert needle in s, needle
    assert "SELECT database_name, source_table, is_pii" in s and "FROM layer_audit_log" in s
    ns = {"__name__": "t"}
    exec(s, ns)                                                              # noqa: S102
    with pytest.raises(SystemExit):
        ns["get_args"](["--catalog", "c"])                                   # all eight arguments are required


def test_all_procedures_ran(loaded):
    bad = {k: v for k, v in loaded.items() if v["status"] != "SUCCESS"}
    assert not bad, bad


def test_merge_keeps_one_current_row_per_key_and_flags_deletes(loaded):
    n_keys = q(f"SELECT count(DISTINCT customer_id) FROM {pq('bronze', 'customer')} WHERE customer_id IS NOT NULL AND trim(customer_id) <> ''")[0][0]
    assert q(f"SELECT count(*) FROM {pq('silver_merge', 'customer_merge')} WHERE report_date = DATE '{RD}'")[0][0] == n_keys
    assert q(f"SELECT count(*) - count(DISTINCT customer_id) FROM {pq('silver_merge', 'customer_merge')}")[0][0] == 0
    assert q(f"SELECT count(*) FROM {pq('silver_merge', 'customer_merge')} WHERE is_deleted NOT IN (0, 1)")[0][0] == 0


def test_transform_matches_existing_silver_and_is_masked(loaded):
    for e in ("customer", "account", "transaction", "branch", "deposit", "loan"):
        old = q(f"SELECT count(*) FROM read_parquet('{LAKE}/silver/{e}/*.parquet')")[0][0]
        new = q(f"SELECT count(*) FROM {pq('silver_transform', e + '_clean')} WHERE report_date = DATE '{RD}'")[0][0]
        assert new == old, (e, old, new)
    assert q(f"SELECT count(*) FROM {pq('silver_transform', 'customer_clean')} WHERE customer_name NOT LIKE '%*%'")[0][0] == 0


def test_every_bronze_table_has_a_clean_table(loaded):
    from banklake.schemas import ENTITIES, SOURCE_ONLY
    for e in list(ENTITIES) + list(SOURCE_ONLY):
        n_m = q(f"SELECT count(*) FROM {pq('silver_merge', e + '_merge')} WHERE report_date = DATE '{RD}'")[0][0]
        n_s = q(f"SELECT count(*) FROM {pq('silver_transform', e + '_clean')} WHERE report_date = DATE '{RD}'")[0][0]
        n_r = q(f"SELECT count(*) FROM {pq('silver_transform', e + '_rejects')} WHERE report_date = DATE '{RD}'")[0][0]
        assert n_s > 0 and n_s + n_r <= n_m, (e, n_m, n_s, n_r)
    assert q(f"SELECT count(*) FROM {pq('silver_transform', 'card_clean')} WHERE card_number NOT LIKE '%*%'")[0][0] == 0


def test_gold_matches_existing_gold_counts_and_stays_masked(loaded):
    gl = lambda t: f"read_parquet('{LAKE}/gold/{t}/*.parquet')"  # noqa: E731
    for old, new in [("customer_360", "gold_customer_360"), ("branch_performance", "gold_branch_performance"),
                     ("deposit_summary", "gold_deposit_summary"), ("loan_summary", "gold_loan_summary"),
                     ("transaction_summary", "gold_transaction_summary")]:
        assert q(f"SELECT count(*) FROM {gl(old)}")[0][0] == q(f"SELECT count(*) FROM {pq('gold', new)}")[0][0], new
    assert q(f"SELECT round(sum(total_balance), 2) FROM {gl('customer_360')}")[0][0] == \
        q(f"SELECT round(sum(total_balance), 2) FROM {pq('gold', 'gold_customer_360')}")[0][0]
    # PII is never decrypted in Gold: masked values stay masked
    assert q(f"SELECT count(*) FROM {pq('gold', 'gold_customer_360')} WHERE customer_name NOT LIKE '%*%'")[0][0] == 0


def test_no_masking_or_unmasking_code_after_bronze():
    for p in catalog.PROCS:
        g = convert.generate(p)
        assert "unmask" not in g["script"].lower() and "vault" not in g["script"].lower(), p["proc_id"]
        assert not re.search(r"\b(mask|encrypt|tokenize)\w*\(", g["script"], re.I), p["proc_id"]


def test_processing_metadata_columns_are_added(loaded):
    cols = {r[0] for r in duckdb.connect().execute(f"DESCRIBE SELECT * FROM {pq('gold', 'gold_customer_overview')}").fetchall()}
    assert {"source_name", "last_upd_date", "processed_date", "business_date", "run_id", "report_date"} <= cols
    assert q(f"SELECT DISTINCT run_id FROM {pq('gold', 'gold_customer_overview')}") == [("ORD-TEST-1",)]


def test_customer_overview_follows_the_spec_example(loaded):
    rows = q(f"SELECT customer_type, total_customers, total_accounts, total_balance FROM {pq('gold', 'gold_customer_overview')}")
    assert rows and all(r[1] > 0 for r in rows)
    assert q(f"SELECT sum(total_accounts) FROM {pq('gold', 'gold_customer_overview')}")[0][0] == \
        q(f"SELECT count(DISTINCT account_id) FROM {pq('silver_transform', 'customer_account_summary')}")[0][0]
    assert set(q(f"SELECT DISTINCT balance_category FROM {pq('silver_transform', 'customer_account_summary')}")) <= {("HIGH",), ("MEDIUM",), ("LOW",)}


def test_rerun_is_idempotent(loaded):
    before = q(f"SELECT count(*) FROM {pq('silver_transform', 'account_clean')}")[0][0]
    assert engine.run_proc("silver_transform.account_clean", RD, "ORD-TEST-2")["status"] == "SUCCESS"
    assert q(f"SELECT count(*) FROM {pq('silver_transform', 'account_clean')}")[0][0] == before


def test_each_layer_is_blocked_until_its_sources_are_loaded(loaded):
    r = engine.run_proc("gold.gold_customer_overview", "2026-02-02", "ORD-TEST-3")
    assert r["status"] == "BLOCKED" and "silver_transform.customer_account_summary" in r["error"]
    r = engine.run_proc("silver_transform.customer_clean", "2026-02-02", "ORD-TEST-3")
    assert r["status"] == "BLOCKED" and "silver_merge.customer" in r["error"]
    assert not q(f"SELECT 1 FROM {pq('gold', 'gold_customer_overview')} WHERE report_date = DATE '2026-02-02'")


def test_audit_log_matches_real_runs(loaded):
    rows = meta.query("SELECT * FROM layer_audit_log WHERE order_id = 'ORD-TEST-1' ORDER BY id")
    assert len(rows) == len(catalog.PROCS) and {r["layer"] for r in rows} == set(catalog.LAYERS)
    for r in rows:
        for f in ("order_id", "layer", "source_table_name", "target_table_name", "step_id", "status", "timestamp", "retry_count", "input_count", "output_count"):
            assert r[f] is not None, f
        n = q(f"SELECT count(*) FROM {pq(r['layer'], r['target_table_name'])} WHERE report_date = DATE '{RD}'")[0][0]
        assert r["output_count"] == n, (r["target_table_name"], r["output_count"], n)       # audit count == rows really in the table
    nb = q(f"SELECT count(*) FROM {pq('bronze', 'customer')}")[0][0]
    r = [x for x in rows if x["target_table_name"] == "customer_merge"][0]
    assert r["input_count"] == nb and r["source_table_name"] == "bronze.customer"


def test_failure_is_audited_and_retried(loaded):
    pid = "silver_transform.branch_clean"
    engine.to_athena(pid)
    art = engine.generate_emr(pid)
    meta.execute("UPDATE proc_artifacts SET script = ? WHERE proc_id = ?", (art["script"].replace("rt.read_table(", "rt.no_such_function("), pid))
    r = engine.run_proc(pid, RD, "ORD-TEST-4", convert_first=False)
    assert r["status"] == "FAILED" and "no_such_function" in r["error"]
    rows = meta.query("SELECT status, retry_count, error_message FROM layer_audit_log WHERE order_id = 'ORD-TEST-4' ORDER BY id")
    assert [(x["status"], x["retry_count"]) for x in rows] == [("FAILED", 0), ("FAILED", 1)]
    engine.generate_emr(pid)


def test_gold_metadata_and_report_list():
    m = meta.query("SELECT * FROM gold_transform_metadata WHERE target_table = 'gold_customer_360' AND is_active = 'Y'")[0]
    assert m["database_name"] == "gold" and "customer_clean" in m["dependencies"]
    assert {"report_name", "report_type", "phase", "load_strategy", "dependencies", "script_path"} <= set(m)
    use = meta.query("SELECT target_table, report_name FROM gold_report_usage")
    assert {u["target_table"] for u in use} == set(catalog.GOLD_INFO)


def _wait():
    for _ in range(600):
        if not engine.running():
            break
        time.sleep(0.5)
    return engine.current()


def test_background_run_of_one_gold_table_runs_missing_upstream_first(loaded):
    rd = "2026-11-24"
    order = engine.start("gold", rd, ["gold_customer_overview"])
    run = _wait()
    assert run["order_id"] == order and run["status"] == "SUCCESS"
    assert [i["proc_id"] for i in run["items"]] == ["silver_merge.customer_merge", "silver_merge.account_merge",
                                                    "silver_transform.customer_account_summary", "gold.gold_customer_overview"]
    assert all(i["status"] == "SUCCESS" for i in run["items"])
    assert engine.loaded("gold_customer_overview", "gold", rd)
    engine.start("gold", rd, ["gold_customer_overview"])                      # everything upstream is loaded now: only the target runs
    assert [i["proc_id"] for i in _wait()["items"]] == ["gold.gold_customer_overview"]


def test_background_layer_run_reports_progress(loaded):
    order = engine.start("gold", RD)
    run = _wait()
    assert run["order_id"] == order and run["status"] == "SUCCESS" and len(run["items"]) == 14
    assert all(i["status"] == "SUCCESS" and i["rows_out"] > 0 for i in run["items"])
