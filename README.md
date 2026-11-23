# BankLake

A metadata-driven banking data lakehouse: ingestion, transformation, PII masking, legacy-SQL migration and
automated source-to-target reconciliation. **All data is synthetic; all code is original.** It is an independent
academic project, not a copy of any client system.

```
raw CSV/JSON ──► BRONZE (Parquet + ingest metadata) ──► SILVER (typed, deduped, MERGE, quarantine) ──► GOLD (reports, PII unmasked)
                       ▲                                        ▲                                          │
                 pipeline_metadata (SQLite) drives every step ──┴── audit logs · reconciliation · watermarks ─┘
                                                                                      Streamlit dashboard ◄──
```

## What is built

| Area | Where | Notes |
|---|---|---|
| Synthetic data + injected anomalies | `src/banklake/datagen.py` | duplicates, null names, bad dates, negative amounts, orphan keys, malformed JSON; manifest of what was injected |
| Bronze ingestion | `bronze.py` | CSV + JSON → Parquet, schema check, full / incremental, corrupt records quarantined |
| Silver (PySpark) | `silver.py` | typing, cleaning, dedupe, referential checks, **MERGE/upsert**, rejected rows → quarantine with a reason |
| Gold (Spark SQL) | `gold.py` | customer 360, branch performance, deposit / loan / transaction summaries |
| Metadata orchestration | `meta.py`, `orchestrator.py` | pipelines, dependencies, load strategy, active flag all live in a table; retries, `BLOCKED` downstream, watermarks |
| PII protection | `masking.py`, `vault.py` | **masked at Bronze ingestion** (value → masked text + token), real values kept encrypted in a separate vault, **role-based unmask** at the Gold/dashboard level, every reveal audited, automatic PII leak scan |
| SQL migration | `sqlmigrate.py` | Teradata-style → Athena rules, unsupported syntax refused, result validated vs PySpark |
| Reconciliation + DQ | `reconcile.py` | row counts, key loss, duplicates, nulls, SUM and row-level value checks, schema types, join funnel, "first failing stage" |
| Audit + monitoring | `meta.py` tables, `dashboard/app.py` | execution log, error log, DQ results, reconciliation results |
| Tests | `tests/` | 20 tests (incremental/idempotency, discrepancy detection + repair, blocking, masking, vault + roles, leak scan, SQL, Gold/BI reconciliation) |

**Not built yet (honest list):** Apache Iceberg tables (MERGE is implemented in PySpark over Parquet instead),
and the AWS pieces (S3, Glue, Athena, EMR, Lambda, Step Functions). The code is layered so these can be added
without changing the logic; see "Next steps".

## Setup (free, nothing to buy)

You need Python 3.10+ and **Java 17** (PySpark needs it). Then:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python main.py demo --scale small  # reset → generate → run all → reconcile → SQL migration
streamlit run dashboard/app.py
python -m pytest tests -q
```

Commands:

```bash
python main.py generate --scale full        # 10k customers, 15k accounts, 100k transactions
python main.py run                          # all pipelines, in dependency order
python main.py run --layers silver gold     # only some layers
python main.py generate --incremental       # new + changed records
python main.py run                          # only new data is processed (watermarks)
python main.py inject --entity account --mode delete --n 25   # break Silver on purpose
python main.py reconcile                    # finds it, names the failing stage
python main.py run --pipelines silver_account --full-refresh  # repair
python main.py sqlmigrate
```

## How PII is handled

```
source file (clear) → BRONZE ingestion → masked value + token ──► Bronze, Silver Merge, Silver Transform: masked only
                            └──► encrypted vault (separate file): token → real value
GOLD customer_360 build ──► reads real values back from the vault (audited as UNMASK_AT_GOLD) ──► Gold and BI: unmasked
```
Masked at Bronze, carried masked through Silver Merge and Silver Transform, unmasked only in Gold (customer_360) and the BI pages built on it.
The reconciliation run includes a leak scan that searches Bronze, Silver Merge and Silver for clear raw values (expected: 0); Gold is not scanned because it is unmasked by design.
Gold customer_360 therefore holds clear (synthetic) PII, so `aws/01_s3_upload.sh` excludes it. The role table (`role_permissions`) still governs direct vault reads.
Demo-grade crypto: the key comes from `BANKLAKE_MASK_KEY`; in production it would sit in a secrets manager / KMS.

## Dashboard pages
| Page | What it shows |
|---|---|
| ETL Pipeline | Source → 1 Ingestion (Bronze) → 2 Transformation (Silver Merge, Silver Transform, Gold) → 3 Reporting (BI), with live row counts and masked/unmasked layers |
| 1 Ingestion | source tables, PII masking rules, Bronze preview |
| 2 Transformation | each Silver and Gold step with input/output/set-aside counts; Teradata procedure → Athena SQL + EMR script |
| 3 Reporting and BI | Gold tables and the executive dashboards |
| Validation | record counts source vs each layer, and the reconciliation checks |

## Demo script (10 minutes)

1. `generate` → show the anomaly manifest.
2. `run` → dashboard Overview: rows per layer, quarantined counts.
3. Dashboard → Data quality: show *why* rows were rejected (reasons).
4. `generate --incremental`, `run` → only new rows processed; run again → nothing processed (idempotent).
5. Inject a discrepancy → Reconcile → "first failing stage: SILVER, keys missing: …" → repair → all PASS.
6. SQL migration page: legacy SQL, converted Athena SQL, result match, the refused `QUALIFY` query.
7. Security page: same customers as `analyst` vs `admin`; Gold has no clear PII.
8. Audit page: break a pipeline (edit metadata) → retries, `FAILED`, downstream `BLOCKED`.

## Next steps

- **Iceberg:** swap the Parquet writers in `silver.py`/`gold.py` for Iceberg tables with `MERGE INTO` (needs the Iceberg Spark runtime jar).
- **AWS (hybrid):** point `common.layer_path` at `s3://` paths, register tables in Glue, query Gold with Athena, run one Spark job on EMR for the cloud demo, trigger ingestion with Lambda, chain pipelines with Step Functions. Set an AWS Budget alert first and shut down EMR after every demo.
- **Benchmarks for the report:** CSV vs Parquet, full vs incremental, Pandas vs PySpark at increasing sizes.

## Confidentiality

Keep this project and your internship report separate. Use fictional names only, write everything from scratch, and
confirm with your Deloitte mentor what may be described before submission. Do not add client names, internal schemas,
procedure names, screenshots, logs or configuration.
