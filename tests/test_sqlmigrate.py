"""Module 7: SQL migration framework - parser, converters, detector, validator, comparator, end-to-end."""
from decimal import Decimal

import pytest

from banklake.sqlmigrate import compare, convert, summarize
from banklake.sqlmigrate.datatypes import Unsupported, convert_type
from banklake.sqlmigrate.parser import calls, match_paren, render, split_args, tokenize
from banklake.sqlmigrate.schema import SchemaMapper
from banklake.sqlmigrate.unsupported import detect_sql
from banklake.sqlmigrate.validator import check_syntax


# ------------------------------------------------------------------ parser
def test_tokenizer_roundtrip_and_protected_text():
    sql = "SEL 'a, b' AS x, \"Q\"\"x\" -- note\nFROM t /* c */"
    toks = tokenize(sql)
    assert render(toks) == sql
    kinds = [t.kind for t in toks]
    assert "string" in kinds and "qident" in kinds and "lcomment" in kinds and "bcomment" in kinds


def test_paren_matching_and_args_with_nesting():
    toks = tokenize("f(a, g(b, c), 'x,y')")
    open_i = next(i for i, t in enumerate(toks) if t.text == "(")
    close = match_paren(toks, open_i)
    args = split_args(toks[open_i + 1:close])
    assert [render(a).strip() for a in args] == ["a", "g(b, c)", "'x,y'"]
    assert [toks[i].text for i, _, _ in calls(toks)] == ["f", "g"]


# ------------------------------------------------------------------ functions
@pytest.mark.parametrize("legacy,expected", [
    ("SEL ZEROIFNULL(a) FROM t", "SELECT COALESCE(a, 0) FROM t"),
    ("SEL NULLIFZERO(a) FROM t", "SELECT NULLIF(a, 0) FROM t"),
    ("SEL NVL(a, 'x') FROM t", "SELECT COALESCE(a, 'x') FROM t"),
    ("SEL NVL2(a, 1, 2) FROM t", "CASE WHEN a IS NOT NULL THEN 1 ELSE 2 END"),
    ("SEL INDEX(a, 'q') FROM t", "strpos(a, 'q')"),
    ("SEL POSITION('q' IN a) FROM t", "strpos(a, 'q')"),
    ("SEL CHARACTERS(a), CHAR_LENGTH(b) FROM t", "length(a), length(b)"),
    ("SEL OREPLACE(a, 'x', 'y') FROM t", "replace(a, 'x', 'y')"),
    ("SEL OTRANSLATE(a, 'x', 'y') FROM t", "translate(a, 'x', 'y')"),
    ("SEL ADD_MONTHS(d, 3) FROM t", "date_add('month', 3, d)"),
    ("SEL LOG(a) FROM t", "log10(a)"),
    ("SEL TO_DATE(s, 'YYYY-MM-DD') FROM t", "CAST(date_parse(s, '%Y-%m-%d') AS DATE)"),
    ("SEL TO_CHAR(d, 'DD/MM/YYYY') FROM t", "date_format(d, '%d/%m/%Y')"),
    ("SEL CSUM(x, d) FROM t", "SUM(x) OVER (ORDER BY d ROWS UNBOUNDED PRECEDING)"),
    ("SEL MAVG(x, 3, d) FROM t", "AVG(x) OVER (ORDER BY d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"),
    ("SEL RANK(x DESC) FROM t", "RANK() OVER (ORDER BY x DESC)"),
    ("SEL a MOD 7 FROM t", "mod(a, 7)"),
])
def test_function_conversions(legacy, expected):
    c = convert(legacy)
    assert expected in c.converted, c.converted
    assert c.status == "CONVERTED", c.unsupported


def test_nested_functions_and_protected_literals():
    c = convert("SEL ZEROIFNULL(SUM(NVL(a, ZEROIFNULL(b)))) AS t, 'ZEROIFNULL(x)' AS lit FROM t")
    assert "COALESCE(SUM(COALESCE(a, COALESCE(b, 0))), 0)" in c.converted
    assert "'ZEROIFNULL(x)'" in c.converted  # text inside a string literal is never rewritten
    assert convert("SEL RANK() OVER (ORDER BY x) FROM t").converted.count("OVER") == 1  # modern RANK left alone


def test_bad_function_arity_is_unsupported_not_guessed():
    c = convert("SEL ZEROIFNULL(a, b) FROM t")
    assert c.status == "UNSUPPORTED" and any("ZEROIFNULL" in u for u in c.unsupported)
    assert convert("SEL TO_DATE(s, fmt_col) FROM t").status == "UNSUPPORTED"
    assert convert("SEL TO_DATE(s, 'DAYS') FROM t").status == "UNSUPPORTED"


def test_top_n_only_on_outermost_select():
    assert convert("SEL TOP 3 a FROM t ORDER BY a").converted.rstrip(";").endswith("LIMIT 3")
    assert convert("SEL a FROM (SEL TOP 3 a FROM t) x").status == "UNSUPPORTED"
    assert convert("SEL TOP 3 a FROM t UNION SEL b FROM u").status == "UNSUPPORTED"


# ------------------------------------------------------------------ data types
@pytest.mark.parametrize("src,dst", [
    ("BYTEINT", "TINYINT"), ("FLOAT", "DOUBLE"), ("REAL", "DOUBLE"), ("DECIMAL(10,2)", "DECIMAL(10, 2)"),
    ("NUMERIC(8)", "DECIMAL(8, 0)"), ("VARCHAR(20)", "VARCHAR(20)"), ("CHAR(3)", "CHAR(3)"),
    ("TIMESTAMP(6)", "TIMESTAMP"), ("CLOB", "VARCHAR"), ("BLOB", "VARBINARY"), ("INT", "INTEGER"),
])
def test_type_conversion(src, dst):
    assert convert_type(src)[0] == dst


def test_type_with_character_set_and_refusals():
    t, note = convert_type("VARCHAR(10) CHARACTER SET UNICODE")
    assert t == "VARCHAR(10)" and "CHARACTER SET" in note
    for bad in ["INTERVAL DAY(2)", "PERIOD(DATE)", "NUMBER", "VARCHAR(5) FORMAT 'X(5)'"]:
        with pytest.raises(Unsupported):
            convert_type(bad)
    c = convert("SEL CAST(a AS FLOAT), CAST(b AS BYTEINT) FROM t")
    assert "CAST(a AS DOUBLE)" in c.converted and "CAST(b AS TINYINT)" in c.converted


# ------------------------------------------------------------------ schema mapping
def test_schema_mapping_explicit_flatten_and_untouched():
    c = convert("SEL * FROM BANKING.CUSTOMER_MASTER m JOIN legacy_db.account a ON m.x = a.x JOIN ods.other o ON o.k = a.k")
    assert "banklake.silver_customer m" in c.converted and "banklake.silver_account a" in c.converted
    assert "ods_other o" in c.converted  # no explicit mapping: schema flattened, and reported
    assert any("schema flattened" in a for a in c.applied)
    c2 = convert("SEL c.legacy_db FROM plain c WHERE c.k = 1")  # column text is not a table reference
    assert "FROM plain c" in c2.converted
    m = SchemaMapper(table_map={"src.t": "dst.t"}, flatten_unknown=False)
    assert "dst.t" in convert("SEL 1 FROM src.t", m).converted and "foo.bar" in convert("SEL 1 FROM foo.bar", m).converted


def test_table_names_inside_strings_and_comments_untouched():
    c = convert("SEL 'legacy_db.account' AS s FROM legacy_db.loan -- legacy_db.customer\n")
    assert "'legacy_db.account'" in c.converted and "banklake.silver_loan" in c.converted
    assert "silver_customer" not in c.converted


# ------------------------------------------------------------------ unsupported detector
@pytest.mark.parametrize("sql,flag", [
    ("SEL a FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY a) = 1", "QUALIFY clause"),
    ("SEL a FROM t SAMPLE 10", "SAMPLE clause"),
    ("CREATE VOLATILE TABLE x AS (SEL 1) WITH DATA", "VOLATILE table"),
    ("COLLECT STATISTICS ON t COLUMN a", "COLLECT STATISTICS"),
    ("SEL a (FORMAT 'YYYY') FROM t", "FORMAT phrase"),
    ("SEL a (NOT CASESPECIFIC) FROM t", "CASESPECIFIC / NOT CASESPECIFIC"),
    ("SEL PERIOD(a, b) FROM t", "PERIOD data type"),
    ("SEL '2026-01-01' (DATE) FROM t", "Teradata type-attribute cast  expr (TYPE)"),
    ("SEL INITCAP(a) FROM t", "INITCAP() (no Athena equivalent)"),
    ("SEL TOP 5 PERCENT a FROM t", "TOP n PERCENT"),
    ("SEL a, SUM(b) OVER (ORDER BY c RESET WHEN b < 0) FROM t", "RESET WHEN (window)"),
])
def test_unsupported_detection(sql, flag):
    assert flag in detect_sql(sql)
    assert convert(sql).status == "UNSUPPORTED"


def test_detector_is_token_aware_and_refuses_non_select():
    assert detect_sql("SEL 'QUALIFY and SAMPLE 5' AS s FROM t -- QUALIFY") == []
    assert detect_sql("SEL sample_rate, volatile_flag FROM t") == []
    assert any("statement type DELETE" in m for m in detect_sql("DELETE FROM t"))
    assert detect_sql("WITH x AS (SEL 1 AS a) SEL a FROM x") == []


# ------------------------------------------------------------------ syntax validator
def test_syntax_validator_accepts_and_rejects():
    assert check_syntax("SELECT a, COALESCE(b, 0) FROM t WHERE c > 1").ok
    bad = check_syntax("SELECT a FROM")
    assert not bad.ok and bad.errors
    assert not check_syntax("SELECT (a FROM t").ok
    assert not check_syntax("SELECT a)) FROM t").ok


def test_converted_output_failing_syntax_check_is_unsupported():
    c = convert("SEL a FROM t WHERE")
    assert c.status == "UNSUPPORTED" and "syntax check" in c.unsupported[0]


# ------------------------------------------------------------------ comparator
def test_comparator_equal_with_decimals_and_tolerance():
    got = [("a", 1, Decimal("10.004")), ("b", 2, 5.0)]
    ref = [("b", 2, 5.0), ("a", 1, 10.0)]
    c = compare(got, ref)
    assert c["match"] and c["row_count_match"] and not c["mismatches"]
    assert "0 record differences" in summarize(c)


def test_comparator_detects_missing_extra_mismatch_and_totals():
    ref = [("a", 1.0), ("b", 2.0), ("c", 3.0)]
    c = compare([("a", 1.0), ("b", 9.0), ("d", 4.0)], ref)
    assert not c["match"] and c["row_count_match"]
    assert c["missing_in_got"] == [("c",)] and c["extra_in_got"] == [("d",)] and c["mismatch_count"] == 1
    assert c["mismatches"][0]["key"] == ("b",)
    assert any(not s["ok"] for s in c["numeric_sums"])
    assert "records differ" in summarize(c)
    assert not compare([("a", 1.0), ("a", 1.0)], [("a", 1.0), ("b", 2.0)])["match"]  # duplicate keys
    assert not compare([("a", None)], [("a", 0)])["match"]  # NULL is not zero


# ------------------------------------------------------------------ end to end (needs a built lake)
def test_end_to_end_all_examples(lake):
    from banklake import meta, sqlmigrate
    res = {r["query"]: r for r in sqlmigrate.run_all()}
    for name in ["legacy_branch_segment_summary", "legacy_recent_accounts", "legacy_customer_report",
                 "legacy_string_null_report", "legacy_running_total", "legacy_top_balances"]:
        assert res[name]["status"] == "CONVERTED" and res[name]["match"] is True, res[name]["detail"]
        assert "syntax OK" in res[name]["detail"] and "0 record differences" in res[name]["detail"]
    for name in ["legacy_unsupported", "legacy_unsupported_format"]:
        assert res[name]["status"] == "UNSUPPORTED" and res[name]["match"] is None
    assert meta.query("SELECT count(*) n FROM sql_migration_log")[0]["n"] >= len(res)


def test_comparator_catches_a_wrong_conversion(lake):
    """A deliberately wrong 'conversion' (drops the SAVINGS filter) must fail comparison with PySpark."""
    from banklake.common import get_spark
    from banklake.sqlmigrate.runner import REFERENCES, _rows, duckdb_connection, duckdb_shim
    legacy = open(__import__("banklake.sqlmigrate", fromlist=["EXAMPLES"]).EXAMPLES / "legacy_branch_segment_summary.sql").read()
    wrong = convert(legacy.replace("AND a.account_type = 'SAVINGS'", ""))
    got = duckdb_connection().execute(duckdb_shim(wrong.converted)).fetchall()
    df, keys = REFERENCES["legacy_branch_segment_summary"](get_spark())
    c = compare(got, _rows(df), key_idx=keys)
    assert c["match"] is False and (c["mismatch_count"] > 0 or not c["row_count_match"])


def test_extract_from_is_not_mistaken_for_a_table():
    """EXTRACT(YEAR FROM d.col) / SUBSTRING(x FROM 2) / TRIM(BOTH ' ' FROM x): the FROM inside the brackets is not a table list."""
    from banklake.sqlmigrate.converter import convert
    c = convert("SEL EXTRACT(YEAR FROM t.opened) y, TRIM(BOTH ' ' FROM t.n) n FROM banking.account_master t")
    assert "EXTRACT(YEAR FROM t.opened)" in c.converted and "t_opened" not in c.converted
    assert "banklake.silver_account" in c.converted
