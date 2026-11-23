# BankLake change plan (from the review list of 23 Nov 2026)

Status of each item is checked against the code in the repo, not against the README.

## 1. Gap analysis

| Area | Today | Gap |
|---|---|---|
| Layers | Raw, Bronze, Silver (clean + merge in one step), Gold | Split Silver into Silver Merge and Silver Transform |
| Datagen | One batch plus a second incremental batch with anomalies | No operation flag (I/U/D), no `source_system`, `created_at`/`updated_at` only on customer and account, no configurable anomaly rates, no deletes |
| Bronze | Parquet, ingestion columns, schema check, corrupt-line quarantine, `ingested_files` | No `load_type`, no `records_rejected` per file, no `status`, no rejected-file handling, no separate ingestion audit table |
| Silver merge | left_anti + union over Parquet, watermark on `_batch_id` | No Iceberg MERGE, no CDC ops, no deletes or soft deletes, merge mixed with cleaning |
| Metadata | pipeline_metadata, dependencies, watermark, masking, roles, ingested_files, execution log | Missing `source_database`, `target_database`, `watermark_column`, `execution_order`; no reload command; no idempotent re-run guard beyond watermark |
| SQL migration | Rule-based converter, `QUALIFY` refused, validated vs PySpark | Not split into parser, function, type and schema converters; no result comparator on counts, keys, sums |
| Validation | 58 checks, discrepancy injection, first failing stage | Stages are Raw/Bronze/Silver/Gold; needs Merge vs Transform stages, join-level counts, column-level comparison, missing-ID trace |
| Security | Masking + token + Fernet vault + role unmask + audit | Fernet is AES-CBC+HMAC, not AES-GCM; masking applied at Bronze, review wants it at Silver Transform; dashboard role dropdown is simulation and must be labelled so |
| Audit | Execution log has run_id, tables, times, rows, retries, status | Missing `batch_id`, `layer`, `failure_reason` column naming; no `transformation_audit_log` / `ingestion_audit_log` |
| AWS | S3 upload done | Glue, Athena, Lambda, Step Functions, EventBridge not built; EMR not built |
| Dashboard | 667-line single file, many pages | Regroup navigation; add trigger-pipeline button |

## 2. Compatibility problems found

1. **Iceberg cannot be tested in the build sandbox.** The Iceberg Spark runtime jar comes from Maven Central, which the sandbox blocks (HTTP 403). Iceberg code would have to be written here but run for the first time on your Mac, where Spark can download the jar.
2. **Region.** The review suggests Mumbai. Your AWS project is locked to Sydney (ap-southeast-2) by an AWS-managed policy that cannot be changed, so all AWS work stays in Sydney.
3. **Masking position.** Moving masking from Bronze to Silver Transform means Bronze would hold clear synthetic PII. That is acceptable only if Bronze never goes to S3. Proposal: Bronze stays local; only Silver Merge onward is uploaded, and Silver Merge keeps the masked columns. See decision A.
4. **Repo layout.** The suggested tree has many folders. I will use a few packages (`silver_merge`, `silver_transform`, `validation`, `security`, `sql_migration`, `aws`) and not dozens of tiny files.

## 3. Decisions needed

- **A. Masking position:** keep masking at Bronze (current, safest for S3) or move to Silver Transform as the review says.
- **B. Iceberg:** write it for the Mac and keep the Parquet merge as the tested local fallback, or skip Iceberg.
- **C. EMR:** the review wants one EMR run. EMR Serverless is the cheaper option; it needs extra IAM permissions.

## 4. Implementation order (one module at a time, tests after each)

1. Datagen v2: batch 1 and batch 2, operation column, `source_system`, timestamps, anomaly rates.
2. Bronze v2: `load_type`, per-file counts, status, rejected files, `ingestion_audit_log`.
3. Silver Merge: new package. Full, incremental, CDC (insert, update, soft delete), dedup, PK check, watermark. Iceberg MERGE behind a flag.
4. Silver Transform: new package. Cleaning, standardisation, joins, business rules, valid and rejected outputs. Unmatched joins are not errors (customer with no loan is valid).
5. Metadata and orchestrator: new columns, `execution_order`, reload command, idempotent re-run.
6. Validation: four stages, join-level counts, missing-ID trace, dashboard view.
7. SQL migration framework split and result comparator.
8. Security: AES-GCM vault, configurable masking rules, role simulation labelled.
9. Audit: column additions, `transformation_audit_log`.
10. Dashboard regroup; Gold lineage document.
11. AWS: Glue catalog, Athena queries, then Step Functions, EventBridge, Lambda, optional EMR.
12. README and reports last, after real test output exists.

## 5. Database changes (SQLite)

- `pipeline_metadata`: add source_database, target_database, watermark_column, execution_order.
- `pipeline_execution_log`: add batch_id, layer; rename error_message to failure_reason (kept as a view alias).
- New: `ingestion_audit_log`, `transformation_audit_log`.
- Unchanged: watermark, ingested_files, masking_metadata, role_permissions, security_audit_log, data_quality_results, reconciliation_results, pipeline_error_log, sql_migration_log.
