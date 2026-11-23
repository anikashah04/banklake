"""Shared paths and Spark session helper."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("BANKLAKE_HOME", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"
RAW = DATA / "raw"
LAKE = DATA / "lake"
DB_PATH = DATA / "banklake_meta.db"

# Test key only. Never reuse real keys; override with an env var.
MASK_KEY = os.environ.get("BANKLAKE_MASK_KEY", "banklake-demo-key-not-a-secret")

_spark = None


def layer_path(layer: str, name: str) -> Path:
    """lake/<layer>/<name>, e.g. layer_path('silver', 'customer')."""
    return LAKE / layer / name


def get_spark():
    """Local Spark session (2 cores). Created once per process."""
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession

        builder = SparkSession.builder
        if os.environ.get("BANKLAKE_USE_ICEBERG") == "1":  # needs the Iceberg jar: downloaded on first start
            builder = (builder
                       .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2")
                       .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                       .config("spark.sql.catalog.bl", "org.apache.iceberg.spark.SparkCatalog")
                       .config("spark.sql.catalog.bl.type", "hadoop")
                       .config("spark.sql.catalog.bl.warehouse", str(LAKE / "iceberg")))
        _spark = (
            builder.master("local[2]")
            .appName("BankLake")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark
