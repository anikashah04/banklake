"""Run the entire pipeline for a chosen date from the Home page: ingest, Silver Merge, Silver Transform, Gold. Runs in a separate process with its own lake."""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(code, tmp_path, timeout=1500):
    env = {**os.environ, "BANKLAKE_HOME": str(tmp_path), "BANKLAKE_SOURCE_SCALE": "small", "PYTHONPATH": f"{ROOT / 'src'}:{ROOT / 'dashboard'}"}
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=timeout)
    assert r.returncode == 0, (r.stdout + r.stderr)[-3000:]
    return r.stdout


def test_entire_pipeline_for_a_date_then_again_then_an_earlier_date(tmp_path):
    out = _run("""
import time
from datetime import date, timedelta
from banklake import meta, pipeline_run
from banklake.transform import catalog
meta.init_db()
try:
    pipeline_run.start(date.today())
    raise SystemExit("future date accepted")
except ValueError as e:
    assert "not available yet" in str(e)
def wait():
    while pipeline_run.running(): time.sleep(0.3)
pipeline_run.start(date(2026, 7, 6))
try:
    pipeline_run.start(date(2026, 7, 6)); raise SystemExit("second start while running was accepted")
except ValueError as e:
    assert "in progress" in str(e)
wait()
r = pipeline_run.current()
assert r["status"] == "SUCCESS", r
assert r["stage"] == "done" and not r["failed"] and not r["skipped"] and r["ingest_status"] in ("SUCCESS", "SKIPPED")
rows = meta.query("SELECT layer, count(DISTINCT target_table_name) n FROM layer_audit_log WHERE report_date='2026-07-06' AND status='SUCCESS' GROUP BY layer")
got = {x["layer"]: x["n"] for x in rows}
assert got == {"silver_merge": 23, "silver_transform": 52, "gold": 14}, got
# the same date again: ingestion is skipped, everything still succeeds
pipeline_run.start(date(2026, 7, 6)); wait()
r2 = pipeline_run.current()
assert r2["status"] == "SUCCESS" and r2["ingest_skipped"], r2
# an earlier date never rolls Bronze back: ingestion is skipped and the layers are built for that date
pipeline_run.start(date(2026, 7, 3)); wait()
r3 = pipeline_run.current()
assert r3["ingest_skipped"] and r3["status"] in ("SUCCESS", "PARTIAL"), r3
print("OK", got)
""", tmp_path)
    assert "OK" in out


def test_home_page_after_a_full_run_points_to_reporting(tmp_path):
    out = _run("""
import time
from datetime import date
from streamlit.testing.v1 import AppTest
from banklake import meta, pipeline_run
meta.init_db()
pipeline_run.start(date(2026, 7, 2))
while pipeline_run.running(): time.sleep(0.3)
assert pipeline_run.current()["status"] == "SUCCESS"
at = AppTest.from_file(%r, default_timeout=180).run()
assert not at.exception, [e.value for e in at.exception]
labels = [b.label for b in at.button]
assert "Run Entire Pipeline" in labels and "Open Reporting" in labels, labels
assert not [b for b in at.button if b.label == "Run Entire Pipeline"][0].disabled
text = " ".join(m.value for m in at.markdown) + " ".join(s.value for s in at.success)
assert "Silver, Gold and every report are ready" in text, text[-500:]
print("OK")
""" % str(ROOT / "dashboard" / "app.py"), tmp_path)
    assert "OK" in out


def test_reports_built_from_a_single_day_explain_that_they_are_empty_instead_of_showing_blanks(tmp_path):
    out = _run("""
import time
from datetime import date
from streamlit.testing.v1 import AppTest
from banklake import meta, ingest
from banklake.transform import engine
meta.init_db()
assert ingest.run(date(2026, 7, 1))["status"] == "SUCCESS"            # one day only: the core tables hold a handful of rows
engine.start("gold", "2026-07-01", None)
while engine.running(): time.sleep(0.5)
at = AppTest.from_file(%r, default_timeout=180).run()
pages = {(i['url_pathname'] or 'home'): h for h, i in at._registered_pages.items() if 'url_pathname' in i}
at._page_hash = pages['reporting']; at.run()
assert not at.exception, [e.value for e in at.exception]
sel = [s for s in at.selectbox if s.label.startswith('Report')][0]
assert any('no data' in o for o in sel.options), sel.options[:3]
def show(name):
    s = [x for x in at.selectbox if x.label.startswith('Report')][0]
    s.select([o for o in s.options if o.startswith(name)][0]).run()
    assert not at.exception, (name, [e.value for e in at.exception])
    return " ".join(w.value for w in at.warning) + " ".join(i.value for i in at.info)
text = show('Customer Overview Dashboard')
assert 'No rows in gold_customer_overview' in text and 'Run Entire Pipeline' in text, text
assert {b.label for b in at.button} >= {'Go to Home', 'Open Ingestion'}
text = show('Fraud Monitoring Dashboard')
assert 'Only 1 row' in text, text
show('Executive Summary')       # mixes empty and filled tables: explains, never crashes
print("OK")
""" % str(ROOT / "dashboard" / "app.py"), tmp_path)
    assert "OK" in out
