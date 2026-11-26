"""Transformation layer driven by procedures: Teradata SELECT -> Athena SELECT -> EMR PySpark script.

  catalog   the procedure definitions (original Teradata SELECTs) and the metadata tables they are registered in
  convert   Teradata SELECT -> Athena SELECT (sqlmigrate converter) -> Spark SQL -> generated EMR script
  runtime   helpers the generated scripts import: metadata queries, table I/O (Iceberg or Parquet fallback), audit log
  engine    converts, generates and runs procedures; per-procedure and whole-layer runs with live progress
"""
