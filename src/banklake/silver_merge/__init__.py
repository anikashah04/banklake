"""Silver Merge: applies Bronze batches (full loads, incremental loads, CDC inserts/updates/deletes)
to a typed-as-source table keyed by primary key. No cleaning happens here - that is Silver Transform."""
import os

from ..common import layer_path

USE_ICEBERG = os.environ.get("BANKLAKE_USE_ICEBERG") == "1"


def table_name(entity: str) -> str:
    return f"bl.silver_merge.{entity}"


def exists(spark, entity: str) -> bool:
    if USE_ICEBERG:
        return spark.catalog.tableExists(table_name(entity))
    return layer_path("silver_merge", entity).exists()


def read(spark, entity: str):
    """The merged table for one entity (including soft-deleted rows, flagged by _is_deleted)."""
    if USE_ICEBERG:
        return spark.table(table_name(entity))
    return spark.read.parquet(str(layer_path("silver_merge", entity)))
