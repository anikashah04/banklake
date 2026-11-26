"""What the generated EMR scripts import. Metadata and audit live in the SQLite metadata database here (MySQL on the real platform);
tables are Iceberg when BANKLAKE_USE_ICEBERG=1, otherwise Parquet folders under data/lake (the local fallback)."""
import os
import shutil
from datetime import datetime

from pyspark.sql import functions as F

from .. import meta
from ..common import layer_path

# logical database -> folder under data/lake (kept apart from the older pipeline's silver/ and gold/ folders)
PHYS = {"bronze": "bronze", "silver_merge": "silver_merge_tables", "silver_transform": "silver_transform_tables", "gold": "gold_tables"}


def table_path(db, table):
    return layer_path(PHYS[db], table)


def configure(args):
    """On EMR the metadata database credentials come from Secrets Manager (args.secret_name); that lookup is not implemented here.
    Locally the metadata database is SQLite."""
    meta.init_db()


def query(sql, params=()):
    return meta.query(sql, tuple(params))


def use_iceberg():
    return os.environ.get("BANKLAKE_USE_ICEBERG") == "1"


def order_id():
    return os.environ.get("BANKLAKE_ORDER_ID") or datetime.now().strftime("ORD-%Y%m%d-%H%M%S")


def retry_count():
    return int(os.environ.get("BANKLAKE_RETRY_COUNT", "0"))


def read_table(spark, catalog, db, table):
    if use_iceberg() and not (db == "bronze" and catalog == "bl"):
        return spark.table(f"{catalog}.{db}.{table}")
    return spark.read.parquet(str(table_path(db, table)))


def count_for_date(df, report_date):
    """Rows read from a source table: the report date's rows when the table has one, otherwise all rows."""
    if "report_date" in df.columns:
        df = df.where(F.col("report_date") == F.to_date(F.lit(report_date)))
    return df.count()


def replace_report_date(spark, df, db, table, report_date):
    """Parquet fallback for DELETE ... WHERE report_date = X followed by an append."""
    p = str(table_path(db, table))
    out = df
    if os.path.isdir(p):
        keep = spark.read.parquet(p).where(F.col("report_date") != F.to_date(F.lit(report_date)))
        out = keep.unionByName(df, allowMissingColumns=True)
    tmp = p + "__tmp"
    out.write.mode("overwrite").parquet(tmp)
    if os.path.isdir(p):
        shutil.rmtree(p)
    os.rename(tmp, p)
    spark.catalog.refreshByPath(p)


def next_step(order):
    return meta.query("SELECT COALESCE(MAX(step_id), 0) + 1 AS step_id FROM layer_audit_log WHERE order_id = ?", (order,))[0]["step_id"]


def audit(order, layer, source, target, step_id, status, retry, n_in, n_out, error, report_date):
    meta.execute("INSERT INTO layer_audit_log(order_id,layer,source_table_name,target_table_name,step_id,status,timestamp,retry_count,"
                 "input_count,output_count,error_message,report_date) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                 (order, layer, source, target, step_id, status, meta.now(), retry, n_in, n_out, error, report_date))
