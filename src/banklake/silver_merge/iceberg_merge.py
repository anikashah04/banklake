"""Apache Iceberg MERGE INTO for Silver Merge (enabled with BANKLAKE_USE_ICEBERG=1).

NOT exercised by the automated tests in the build sandbox (the Iceberg jar cannot be downloaded there).
It must be run and verified on a machine that can fetch the jar; see docs/ICEBERG.md.
"""
from . import table_name


def merge(spark, entity: str, rows, key: str, overwrite: bool = False) -> None:
    tbl = table_name(entity)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bl.silver_merge")
    if overwrite or not spark.catalog.tableExists(tbl):
        rows.writeTo(tbl).using("iceberg").createOrReplace()
        return
    rows.createOrReplaceTempView("_incoming_changes")
    spark.sql(f"MERGE INTO {tbl} t USING _incoming_changes s ON t.{key} = s.{key} "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
