"""End-to-end SQL migration run: convert -> syntax check -> execute on DuckDB over Silver -> compare with PySpark reference."""
import json
import re
from pathlib import Path

import duckdb

from .. import meta
from ..common import DATA, LAKE
from .comparator import compare, summarize
from .converter import Conversion, convert

EXAMPLES = Path(__file__).resolve().parents[3] / "sql_examples"  # ships with the code


def duckdb_connection():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA banklake")
    for e in ["customer", "account", "transaction", "branch", "deposit", "loan"]:
        p = LAKE / "silver" / e
        if any(p.glob("*.parquet")):
            con.execute(f"CREATE VIEW banklake.silver_{e} AS SELECT * FROM read_parquet('{p}/*.parquet')")
    return con


def duckdb_shim(sql: str) -> str:
    """Athena's date_add('month', n, d) has no direct DuckDB twin - rewrite for local execution only."""
    return re.sub(r"date_add\('month',\s*(-?\d+),\s*(DATE\s*'[^']*'|[\w.]+)\)", r"(\2 + INTERVAL (\1) MONTH)", sql)


# ---------------------------------------------------------------- independent PySpark references
def _tables(spark):
    return {e: spark.read.parquet(str(LAKE / "silver" / e)) for e in ["customer", "account", "transaction", "branch"]}


def ref_branch_segment_summary(spark):
    from pyspark.sql import functions as F
    t = _tables(spark)
    return (t["account"].join(t["customer"], "customer_id").join(t["branch"], "branch_id")
            .filter((F.col("opened_date") <= F.lit("2026-06-30").cast("date")) & (F.col("account_type") == "SAVINGS"))
            .groupBy("region", "segment")
            .agg(F.count("*").alias("n_accounts"), F.coalesce(F.sum("balance"), F.lit(0)).alias("total_balance"),
                 F.avg("balance").cast("decimal(18,2)").alias("avg_balance"))
            .orderBy("region", "segment")), (0, 1)


def ref_recent_accounts(spark):
    from pyspark.sql import functions as F
    t = _tables(spark)
    return (t["account"].join(t["customer"], "customer_id")
            .filter(F.col("opened_date") >= F.add_months(F.lit("2026-06-30").cast("date"), -24))
            .groupBy("account_type")
            .agg(F.count("*").alias("n_accounts"), F.coalesce(F.sum("balance"), F.lit(0)).alias("total_balance"),
                 F.sum(F.when(F.instr(F.col("city"), "a") > 0, 1).otherwise(0)).alias("cities_with_a"))
            .orderBy("account_type")), (0,)


def ref_customer_report(spark):
    from pyspark.sql import functions as F
    t = _tables(spark)
    return t["customer"].select("customer_id", F.current_date().alias("report_date")).orderBy("customer_id"), (0,)


def ref_string_null_report(spark):
    from pyspark.sql import functions as F
    t = _tables(spark)
    return (t["customer"].groupBy("segment")
            .agg(F.count("*").alias("n_customers"),
                 F.sum(F.length(F.coalesce(F.col("city"), F.lit("")))).alias("total_city_chars"),
                 F.avg(F.length("customer_name")).alias("avg_name_len"))
            .orderBy("segment")), (0,)


def ref_running_total(spark):
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    t = _tables(spark)
    m = (t["transaction"].filter(F.col("transaction_date").isNotNull())
         .withColumn("m", F.year("transaction_date") * 100 + F.month("transaction_date"))
         .groupBy("m").agg(F.sum("amount").alias("monthly_amount")))
    w = Window.orderBy("m").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return m.withColumn("running_amount", F.sum("monthly_amount").over(w)).orderBy("m"), (0,)


def ref_top_balances(spark):
    from pyspark.sql import functions as F
    t = _tables(spark)
    return (t["account"].join(t["customer"], "customer_id").groupBy("customer_id")
            .agg(F.coalesce(F.sum("balance"), F.lit(0)).alias("total_balance"))
            .orderBy(F.col("total_balance").desc(), F.col("customer_id")).limit(5)), (0,)


REFERENCES = {
    "legacy_branch_segment_summary": ref_branch_segment_summary,
    "legacy_recent_accounts": ref_recent_accounts,
    "legacy_customer_report": ref_customer_report,
    "legacy_string_null_report": ref_string_null_report,
    "legacy_running_total": ref_running_total,
    "legacy_top_balances": ref_top_balances,
}


def _rows(df):
    return [tuple(x for x in r) for r in df.collect()]


def validate(conv: Conversion, name: str, spark=None) -> dict:
    if conv.unsupported:
        return dict(match=None, detail=f"not executed - unsupported syntax: {conv.unsupported}")
    con = duckdb_connection()
    got = con.execute(duckdb_shim(conv.converted)).fetchall()
    syntax = "syntax OK" if conv.syntax and conv.syntax.ok else "syntax not checked"
    ref_fn = REFERENCES.get(name)
    if spark is None or ref_fn is None:
        return dict(match=None, detail=f"{syntax}; executed OK on DuckDB ({len(got)} rows); no PySpark reference registered")
    df, keys = ref_fn(spark)
    cmp = compare(got, _rows(df), key_idx=keys)
    return dict(match=cmp["match"], detail=f"{syntax}; DuckDB(Athena SQL) vs PySpark: " + summarize(cmp), comparison=cmp)


def run_all(spark=None) -> list[dict]:
    spark = spark or __import__("banklake.common", fromlist=["get_spark"]).get_spark()
    out = []
    gen = DATA / "generated_sql"
    gen.mkdir(parents=True, exist_ok=True)
    for f in sorted(EXAMPLES.glob("legacy_*.sql")):
        conv = convert(f.read_text())
        v = validate(conv, f.stem, spark)
        (gen / f"athena_{f.stem[len('legacy_'):]}.sql").write_text(conv.converted + "\n")
        meta.execute("INSERT INTO sql_migration_log(ts,query_name,legacy_sql,converted_sql,applied,unsupported,status,match,detail) VALUES(?,?,?,?,?,?,?,?,?)",
                     (meta.now(), f.stem, conv.legacy, conv.converted, json.dumps(conv.applied), json.dumps(conv.unsupported),
                      conv.status, None if v["match"] is None else int(v["match"]), v["detail"]))
        out.append(dict(query=f.stem, status=conv.status, rules_applied=conv.applied, unsupported=conv.unsupported,
                        match=v["match"], detail=v["detail"]))
    return out
