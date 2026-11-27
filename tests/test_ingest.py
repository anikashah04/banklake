"""Source ingestion: persistent source databases, load-frequency rules, Bronze transformations, live state, idempotency, failures.
The scenarios run once in a separate process with its own BANKLAKE_HOME so the shared test lake is untouched."""
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent / "_ingest_scenarios.py"


@pytest.fixture(scope="module")
def sc():
    env = dict(os.environ, BANKLAKE_HOME=tempfile.mkdtemp(prefix="banklake_ingest_"))
    p = subprocess.run([sys.executable, str(SCRIPT)], env=env, capture_output=True, text=True, timeout=1500)
    assert p.returncode == 0, p.stderr[-2000:]
    m = re.search(r"@@(\{.*\})", p.stdout + p.stderr)
    assert m, (p.stdout + p.stderr)[-2000:]
    return json.loads(m.group(1))


def test_three_persistent_sources_with_real_records(sc):
    names = {k.split("/")[0] for k in sc["sources"]}
    assert names == {"SOURCE 1", "SOURCE 2", "SOURCE 3"}
    assert all(n > 0 for n in sc["sources"].values())


def test_source_data_is_not_regenerated_by_runs(sc):
    assert sc["customer_source_after"] == sc["customer_source_before"]   # only the one row the test added


def test_load_frequency_rules(sc):
    due = {d: {(e, f) for e, f, _ in v} for d, v in sc["due"].items()}
    assert {f for _, f in due["2026-04-15"]} == {"DAILY"}                                    # mid-month: daily only
    assert {f for _, f in due["2026-06-02"]} == {"DAILY"}
    assert {f for _, f in due["2026-05-01"]} == {"DAILY", "MONTHLY"}                         # 1 May: daily + April close
    assert {f for _, f in due["2026-07-01"]} == {"DAILY", "MONTHLY", "QUARTERLY"}            # 1 July: all three
    keys = {e: k for e, f, k in sc["due"]["2026-07-01"]}
    assert keys["account"] == "D:2026-07-01" and keys["loan"] == "M:2026-06-30" and keys["deposit"] == "Q:2026-06-30"
    assert dict((e, k) for e, f, k in sc["due"]["2026-05-01"])["loan"] == "M:2026-04-30"
    assert dict((e, k) for e, f, k in sc["due"]["2026-01-01"])["branch"] == "Q:2025-12-31"   # quarter close crosses the year


def test_counts_come_from_the_real_source_and_bronze(sc):
    assert sc["run_jul1"]["status"] == "SUCCESS"
    for entity, _freq, status, source, extracted, transformed, inserted, bronze in sc["run_jul1"]["tables"]:
        assert status == "SUCCESS"
        assert source == extracted == transformed == inserted == bronze == sc["expected_jul1"][entity]   # expected = independent query of the source DB


def test_flag_columns_are_trimmed_and_uppercased(sc):
    assert sc["mask"]["segment"] == "RETAIL"      # source value was ' retail '


def test_pii_is_masked_with_the_existing_vault(sc):
    m = sc["mask"]
    assert "lovelace" not in m["name"] and "ada@" not in m["email"] and "9876" not in m["phone"]
    assert m["token_cols"] == ["customer_name_token", "email_token", "phone_token"]
    assert m["token_ok"]                                           # same token function as the Spark path
    assert list(m["revealed"].values()) == [" ada lovelace "]      # decrypts from the AES-GCM vault


def test_arrow_colours_follow_the_backend_stage(sc):
    assert sc["arrows_first_extract"]["transform"] == "waiting" and sc["arrows_first_extract"]["insert"] == "waiting"
    a = sc["arrows_first_transform"]
    assert a["S1"] == "processing" and a["S2"] == "waiting" and a["S3"] == "waiting"   # only the source being read is active
    assert a["transform"] == "processing" and a["insert"] == "waiting"
    z = sc["arrows_last_insert"]
    assert [z["S1"], z["S2"], z["S3"], z["transform"], z["insert"]] == ["done", "done", "done", "done", "processing"]
    assert set(sc["arrows_end"].values()) == {"done"}


def test_rerun_is_idempotent(sc):
    r = sc["rerun"]
    assert set(r["statuses"]) == {"SKIPPED"} and r["status"] == "SUCCESS"
    assert r["same_rows"] and r["success_rows_added"] == 0


def test_silver_and_gold_run_on_the_new_bronze(sc):
    assert sc["downstream"] == ["SUCCESS"]


def test_table_failure_is_reported_not_hidden(sc):
    f = sc["fail_stage"]
    assert f["status"] == "PARTIAL"
    assert f["tables"]["account"][:2] == ["FAILED", "transform"] and "boom" in f["tables"]["account"][2]
    assert f["tables"]["customer"][0] == f["tables"]["transaction"][0] == "SUCCESS"
    assert f["arrow_transform"] == "failed"
    assert f["pipeline_log"][-1] == "FAILED" and f["audit_failed"] == 1     # downstream stays blocked for that table


def test_retry_loads_only_the_failed_table_without_duplicates(sc):
    r = sc["fail_retry"]
    assert r["status"] == "SUCCESS" and r["tables"]["account"] == "SUCCESS" and {v for k, v in r["tables"].items() if k != "account"} == {"SKIPPED"}
    assert r["tx_unchanged"] and r["pipeline_log"][-2:] == ["FAILED", "SUCCESS"]


def test_schema_mismatch_fails_only_that_table(sc):
    f = sc["schema_fail"]
    assert f["status"] == "PARTIAL" and f["tables"]["loan"][0] == "FAILED" and "schema mismatch" in f["tables"]["loan"][1]
    assert f["tables"]["account"][0] == "SUCCESS"
    assert sc["schema_fixed"]["tables"]["loan"] == "SUCCESS" and sc["schema_fixed"]["status"] == "SUCCESS"


def test_batch_ids_increase(sc):
    b = sc["batch_ids_increase"]
    assert b == sorted(b) and len(set(b)) == len(b)


def test_twenty_three_banking_tables_with_many_columns(sc):
    r = sc["rich"]
    assert r["tables"] == 23
    assert len(r["customer_cols"]) >= 27 and {"date_of_birth", "gender", "risk_rating", "kyc_status"} <= set(r["customer_cols"])
    assert all(n > 0 for n in r["source_only_bronze"].values())      # card, KYC, address, interaction, repayment, beneficiary reached Bronze


def test_flags_cleaned_and_pii_masked_on_every_table(sc):
    r = sc["rich"]
    assert r["dirty_flags"] == 0                                      # padded / lower-case source values were trimmed and upper-cased
    assert r["kyc_id"].startswith("******") and r["card"].startswith("******")
    assert set(r["address"]) == {"*"}
    assert r["bene"][0].replace("*", "").replace(" ", "") and "*" in r["bene"][0] and r["bene"][1].startswith("******")


def test_future_business_dates_are_rejected(sc):
    assert "available only up to" in sc["future"] and sc["future_start"] == "REJECTED"


def test_ingestion_date_picker_stops_at_yesterday(tmp_path):
    code = f"""
import datetime
from streamlit.testing.v1 import AppTest
at = AppTest.from_file({str(Path(__file__).resolve().parents[1] / 'dashboard' / 'app.py')!r}, default_timeout=180).run()
pages = {{(i['url_pathname'] or 'overview'): h for h, i in at._registered_pages.items() if 'url_pathname' in i}}
at._page_hash = pages['ingestion']; at.run()
assert not at.exception
assert not [b for b in at.button if b.label == 'Start Ingestion'][0].disabled
assert at.date_input[0].max == datetime.date.today() - datetime.timedelta(days=1), at.date_input[0].max
assert any(b.label == 'Load History' for b in at.button)
"""
    env = {**os.environ, "BANKLAKE_HOME": str(tmp_path)}
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=900)
    assert r.returncode == 0, (r.stdout + r.stderr)[-2000:]


def test_load_history_brings_in_every_record_up_to_the_date_and_daily_loads_continue(tmp_path):
    code = """
import duckdb
from datetime import date
from banklake import meta, sources, ingest
from banklake.common import LAKE
meta.init_db(); sources.ensure_sources()
h = sources.due_loads(date(2026, 7, 15), history=True)
assert len(h) == 23 and all(l.snapshot for l in h) and {l.period_key for l in h if l.frequency == 'MONTHLY'} == {'M:2026-06-30'}
assert {l.period_key for l in h if l.frequency == 'QUARTERLY'} == {'Q:2026-06-30'}
r = ingest.run(date(2026, 7, 15), history=True)
assert r['status'] == 'SUCCESS', r['status']
con = sources.connect('SOURCE 2'); n_src = con.execute("SELECT count(*) FROM customer WHERE business_date <= '2026-07-15'").fetchone()[0]; con.close()
n_b = duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{LAKE}/bronze/customer/*.parquet')").fetchone()[0]
assert n_b == n_src and n_b > 50, (n_b, n_src)
r2 = ingest.run(date(2026, 7, 15), history=True)                    # same date again: nothing is loaded twice
assert all(t['status'] == 'SKIPPED' for t in r2['tables'])
r3 = ingest.run(date(2026, 7, 16))                                  # the next normal day continues from there
assert r3['status'] == 'SUCCESS'
n_after = duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{LAKE}/bronze/customer/*.parquet')").fetchone()[0]
assert n_after >= n_b
"""
    env = {**os.environ, "BANKLAKE_HOME": str(tmp_path), "BANKLAKE_SOURCE_SCALE": "small", "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=900)
    assert r.returncode == 0, (r.stdout + r.stderr)[-2000:]


def test_every_business_date_has_records_for_every_daily_table(sc):
    cov = sc["coverage"]
    assert len(cov["daily"]) >= 12
    assert all(n == cov["days"] for n in cov["daily"].values()), cov


def test_sources_have_symbols(sc):
    assert set(sc["symbols"]) == {"SOURCE 1", "SOURCE 2", "SOURCE 3"} and all(v.isupper() for v in sc["symbols"].values())


def test_dry_run_shows_the_steps_and_writes_nothing(sc):
    assert sc["dry_titles"] == ["Check what is due", "Extract the record from the source", "Trim and upper-case flag columns",
                                "Mask PII with the AES vault", "Add Bronze metadata", "Row ready for Bronze"]
    assert sc["dry_nowrite"] == {"vault_same": True, "bronze_absent": True, "log_same": True}
    assert sc["dry_future"] == "REJECTED"


def test_dry_run_row_is_exactly_the_row_a_real_ingestion_inserts(sc):
    assert all(sc["dry_matches_real"].values()), sc["dry_matches_real"]
