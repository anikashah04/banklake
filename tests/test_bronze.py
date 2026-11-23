"""Bronze v2: ingestion audit log, source-to-Bronze counts, duplicate-file detection, rejected files."""
import shutil

from banklake import meta, orchestrator
from banklake.common import RAW, layer_path
from banklake.schemas import ENTITIES


def test_every_source_file_has_an_audit_row_with_consistent_counts(lake):
    rows = meta.query("SELECT * FROM ingestion_audit_log WHERE status='SUCCESS'")
    assert {r["entity"] for r in rows} >= set(ENTITIES)
    for r in rows:
        assert r["records_read"] == r["records_loaded"] + r["records_rejected"]
        assert r["load_type"] in ("FULL", "INCREMENTAL") and r["file_hash"] and r["run_id"]
    txn = [r for r in rows if r["entity"] == "transaction" and r["source_file"] == "transaction_full.json"][0]
    assert txn["records_rejected"] == 3  # the three malformed JSON lines planted by the generator
    # audit totals agree with the file tracker
    for e in ENTITIES:
        a = meta.query("SELECT sum(records_loaded) n FROM ingestion_audit_log WHERE entity=? AND status='SUCCESS'", (e,))[0]["n"]
        assert a >= meta.query("SELECT sum(rows_loaded) n FROM ingested_files WHERE entity=?", (e,))[0]["n"]


def test_duplicate_file_content_is_not_loaded_twice(lake):
    import duckdb
    src, dup = RAW / "loan" / "loan_full.csv", RAW / "loan" / "loan_zcopy.csv"

    def n_bronze():
        return duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{layer_path('bronze', 'loan')}/*.parquet')").fetchone()[0]
    before = n_bronze()
    shutil.copy(src, dup)
    try:
        st = orchestrator.run(pipelines=["bronze_loan"], retry_sleep=0)["status"]
        assert st["bronze_loan"] == "SUCCESS"
        assert n_bronze() == before
        row = meta.query("SELECT status, message FROM ingestion_audit_log WHERE source_file='loan_zcopy.csv' ORDER BY id DESC LIMIT 1")[0]
        assert row["status"] == "SKIPPED" and "duplicate" in row["message"]
    finally:
        dup.unlink()


def test_bad_file_is_rejected_but_valid_files_still_load(lake):
    bad = RAW / "customer" / "customer_inc_77.csv"
    bad.write_text("customer_id,customer_name\nC1,X\n")
    try:
        st = orchestrator.run(pipelines=["bronze_customer"], retry_sleep=0)["status"]
        assert st["bronze_customer"] == "FAILED"
        assert (layer_path("quarantine", "rejected_files") / "customer" / bad.name).exists()
        row = meta.query("SELECT status, message FROM ingestion_audit_log WHERE source_file=? ORDER BY id DESC LIMIT 1", (bad.name,))[0]
        assert row["status"] == "FAILED" and "Schema mismatch" in row["message"]
        assert not meta.query("SELECT 1 FROM ingested_files WHERE file_name=?", (bad.name,))
    finally:
        bad.unlink()
        orchestrator.run(pipelines=["bronze_customer"], retry_sleep=0)
