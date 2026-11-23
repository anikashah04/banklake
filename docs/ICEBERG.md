# Running Silver Merge on Apache Iceberg

Silver Merge has two storage back ends behind one switch.

| Mode | How to select | Status |
|---|---|---|
| Parquet merge (default) | nothing | Tested: 32 automated tests pass |
| Iceberg `MERGE INTO` | `export BANKLAKE_USE_ICEBERG=1` | Code written, **not yet run**: the build sandbox cannot download the Iceberg jar |

## First run on a machine with internet access
```
export BANKLAKE_USE_ICEBERG=1
python main.py reset
python main.py generate --scale small
python main.py run --layers bronze silver_merge
```
Spark downloads `iceberg-spark-runtime-3.5_2.12:1.5.2` on the first start. The tables are created in a Hadoop
catalog named `bl` under `data/lake/iceberg`, as `bl.silver_merge.<entity>`.

## What to check
- `python main.py run` ends with SUCCESS for every `merge_*` pipeline.
- Re-running the same command processes 0 rows.
- After `python main.py generate --incremental` and another run, the audit table shows inserts, updates and deletes
  (`SELECT * FROM transformation_audit_log` in `data/banklake_meta.db`).

Until this has been run once, do not describe Iceberg as working in the report.
