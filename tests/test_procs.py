"""Teradata procedure -> Athena SQL + EMR script."""
from pathlib import Path

import sqlglot

from banklake.sqlmigrate import procs

EX = Path(__file__).resolve().parents[1] / "sql_examples" / "proc_refresh_customer_summary.sql"


def test_example_procedure_converts_and_procedural_parts_are_flagged():
    r = procs.convert_procedure(EX.read_text())
    assert r.name == "refresh_customer_summary" and r.params == ["IN p_branch VARCHAR(10)"]
    assert r.counts == {"CONVERTED": 3, "UNSUPPORTED": 0, "MANUAL": 2}
    kinds = [(s.status, s.legacy.split()[0].upper()) for s in r.statements]
    assert kinds[0] == ("MANUAL", "DECLARE") and ("MANUAL", "SET") in kinds


def test_dml_is_rewritten_like_plain_queries_and_parses_in_both_dialects():
    r = procs.convert_procedure(EX.read_text())
    ins = [s for s in r.statements if s.converted.upper().startswith("INSERT")][0]
    assert "COALESCE(SUM(BALANCE), 0)" in ins.converted and "SEL " not in ins.converted and "banking_account" in ins.converted
    assert [s.converted for s in r.statements if s.converted.upper().startswith("DELETE")] == ["DELETE FROM banking_customer_summary;"]
    for s in r.statements:
        if s.status == "CONVERTED":
            sqlglot.parse_one(s.converted.rstrip(";"), read="trino")
            sqlglot.parse_one(s.spark, read="spark")


def test_generated_emr_script_is_valid_python_and_keeps_manual_steps_as_comments():
    r = procs.convert_procedure(EX.read_text())
    ok, err = procs.script_compiles(r.script)
    assert ok, err
    assert "NOT been run on EMR" in r.script and "# TODO: DECLARE" in r.script and r.script.count("spark.sql(") == 3


def test_unsupported_statement_is_reported_not_guessed():
    r = procs.convert_procedure("CREATE PROCEDURE p() BEGIN UPDATE t FROM u SET a=1; MERGE INTO t USING u ON t.a=u.a WHEN MATCHED THEN UPDATE SET b=1; END;")
    assert all(s.status != "CONVERTED" for s in r.statements)
