"""Teradata SELECT -> Athena SELECT -> Spark SQL -> EMR PySpark script.

Step 1  Athena: the existing sqlmigrate converter (SEL -> SELECT, ZEROIFNULL -> COALESCE, TO_CHAR -> date_format, schema dots -> underscores,
        syntax check), then two procedure-level rules: the report-date placeholder becomes DATE('{args.report_date}') and CAST becomes TRY_CAST.
Step 2  Spark SQL: the Athena SELECT transpiled Trino -> Spark with sqlglot (this is what the EMR job runs against its temp views).
Step 3  Script: a PySpark job with the standard argument pattern, metadata look-ups, temp views, the report-date DELETE, the append and the audit row.
"""
import re

import sqlglot

from ..sqlmigrate.converter import convert
from . import catalog

PLACEHOLDER = "DATE '{report_date}'"
ATHENA_DATE = "DATE('{args.report_date}')"
SENTINEL = "DATE '1900-01-01'"
ARGS = ("catalog", "source_db", "target_tables", "report_date", "secret_name", "env", "region", "script_name")


def table_alias(db, table):
    """Name of a source table inside Athena SQL and as an EMR temp view: schema dots are replaced by underscores."""
    return f"{db}_{table}"


def to_athena(proc):
    """Returns dict(sql, status, notes, problems). SELECT only: the converter rejects any other statement type."""
    c = convert(proc["teradata"])
    sql = c.converted.rstrip().rstrip(";")
    notes = list(dict.fromkeys(c.applied))
    if PLACEHOLDER in sql:
        sql = sql.replace(PLACEHOLDER, ATHENA_DATE)
        notes.append("report date: DATE '{report_date}' -> DATE('{args.report_date}')")
    n = len(re.findall(r"\bCAST\(", sql))
    if n:
        sql = re.sub(r"(?<![\w])CAST\(", "TRY_CAST(", sql)
        notes.append(f"CAST -> TRY_CAST ({n}): Athena fails the whole query on one bad value; TRY_CAST gives NULL and the row is filtered by the validity rule")
    problems = list(c.unsupported)
    if not problems and not re.match(r"^\s*(SELECT|WITH)\b", sql, re.I):
        problems.append("not a SELECT")
    if not problems and re.search(r"\b(INSERT|UPDATE|DELETE|MERGE)\b", sql, re.I):
        problems.append("contains a write statement; procedures must be SELECT only")
    return dict(sql=sql, status="UNSUPPORTED" if problems else "CONVERTED", notes=notes, problems=problems)


def to_spark(athena_sql):
    """Trino -> Spark SQL. The report-date placeholder is protected during the transpile and put back as DATE('{args.report_date}')."""
    tmp = athena_sql.replace(ATHENA_DATE, SENTINEL)
    out = sqlglot.transpile(tmp, read="trino", write="spark", pretty=True)[0]
    out = re.sub(r"CAST\('1900-01-01' AS DATE\)|DATE '1900-01-01'|TO_DATE\('1900-01-01'\)", ATHENA_DATE, out)
    return out


def script_name(proc):
    return proc["script_name"]


def build_script(proc, spark_sql):
    layer, target = proc["layer"], proc["target"]
    if layer == "gold":
        meta_sql = ("SELECT database_name, source_table, is_pii, pii_entity, pii_columns FROM gold_transform_metadata "
                    "WHERE target_table = ? AND is_active = 'Y'")
        meta_args = "(target,)"
    else:
        meta_sql = ("SELECT database_name, source_table, is_pii, pii_entity, pii_columns FROM silver_transform_metadata "
                    "WHERE layer = ? AND target_table = ? AND is_active = 'Y'")
        meta_args = "(LAYER, target)"
    body = "\n".join("            " + ln if ln else "" for ln in spark_sql.splitlines())
    return f'''"""EMR PySpark job: {target}  ({catalog.LAYER_TITLE[layer]}).

Generated from the Teradata procedure {proc["proc_id"]} -> Athena SELECT -> Spark SQL.
Teradata and Athena hold the SELECT only. This job does the processing: it reads the source tables, runs the SELECT with
Spark SQL, deletes the rows of the report date from the target Iceberg table, appends the new rows, and writes the audit log.

Run on EMR:
  spark-submit {proc["script_name"]}.py --catalog glue_catalog --source_db {proc["source_db"]} --target_tables {target} \\
      --report_date 2026-07-01 --secret_name <secret> --env dev --region ap-southeast-2 --script_name {proc["script_name"]}
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from banklake.transform import runtime as rt   # metadata queries, table reads, Parquet fallback, audit log

LAYER = "{layer}"


def get_args(argv=None):
    p = argparse.ArgumentParser(description="{target}")
    for name in {list(ARGS)!r}:
        p.add_argument("--" + name, required=True)
    return p.parse_args(argv)


def build_spark(args):
    """Iceberg on the Glue catalog (the project's existing catalog settings)."""
    cat = "spark.sql.catalog." + args.catalog
    return (SparkSession.builder.appName(args.script_name)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(cat, "org.apache.iceberg.spark.SparkCatalog")
            .config(cat + ".catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(cat + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(cat + ".warehouse", f"s3://id-aws-{{args.env}}-s3-gold-pii/data/warehouse/")
            .getOrCreate())


def main(spark, args):
    rt.configure(args)
    target = args.target_tables
    order_id, retry = rt.order_id(), rt.retry_count()
    iceberg = rt.use_iceberg()

    # 1. metadata: target database, source tables, PII flag (no table names are hard-coded)
    meta = rt.query("{meta_sql}", {meta_args})[0]
    sources = meta["source_table"].split(",")
    # 2. latest audit step id for this order
    step_id = rt.query("SELECT COALESCE(MAX(step_id), 0) + 1 AS step_id FROM layer_audit_log WHERE order_id = ?", (order_id,))[0]["step_id"]
    src_label = ",".join(f"{{args.source_db}}.{{t}}" for t in sources)
    n_in = 0
    try:
        # 3. read the source tables and create temp views (the SELECT refers to them as <database>_<table>)
        for t in sources:
            df = rt.read_table(spark, args.catalog, args.source_db, t)
            df.createOrReplaceTempView(f"{{args.source_db}}_{{t}}")
            n_in += rt.count_for_date(df, args.report_date)

        # 4. the transformation SELECT (converted from Teradata via Athena)
        df = spark.sql(f"""
{body}
        """)

        # 5. processing metadata columns
        df = (df.withColumn("source_name", F.lit(src_label))
                .withColumn("last_upd_date", F.current_timestamp())
                .withColumn("processed_date", F.current_date())
                .withColumn("business_date", F.to_date(F.lit(args.report_date)))
                .withColumn("run_id", F.lit(order_id)))

        # 6. PII stays masked: tokens were created in Bronze and are never reversed here

        df = df.cache()
        n_out = df.count()

        # 7. replace the report date in the target table
        db = meta["database_name"]
        full = f"{{args.catalog}}.{{db}}.{{target}}"
        if iceberg:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {{args.catalog}}.{{db}}")
            if not spark.catalog.tableExists(full):
                df.limit(0).writeTo(full).using("iceberg").create()
            spark.sql(f"DELETE FROM {{full}} WHERE report_date = DATE('{{args.report_date}}')")
            df.writeTo(full).append()
        else:                                  # local run: Parquet fallback, same delete-then-append behaviour
            rt.replace_report_date(spark, df, db, target, args.report_date)

        rt.audit(order_id, LAYER, src_label, target, step_id, "SUCCESS", retry, n_in, n_out, None, args.report_date)
        return dict(rows_in=n_in, rows_out=n_out)
    except Exception as e:  # noqa: BLE001 - audit the failure, then let the caller see it
        rt.audit(order_id, LAYER, src_label, target, step_id, "FAILED", retry, n_in, 0, str(e)[:500], args.report_date)
        raise


if __name__ == "__main__":
    a = get_args()
    os.environ["BANKLAKE_USE_ICEBERG"] = "1"
    main(build_spark(a), a)
'''


def generate(proc):
    """Convert one procedure end to end. Returns dict(athena, notes, status, problems, emr_sql, script)."""
    a = to_athena(proc)
    out = dict(athena=a["sql"], status=a["status"], notes=a["notes"], problems=a["problems"], emr_sql="", script="")
    if a["status"] == "CONVERTED":
        out["emr_sql"] = to_spark(a["sql"])
        out["script"] = build_script(proc, out["emr_sql"])
        compile(out["script"], proc["script_name"] + ".py", "exec")
    return out
