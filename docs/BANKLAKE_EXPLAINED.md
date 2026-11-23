# BankLake explained

A plain-language guide to what was built, what goes in, what comes out, how it looks, and what every table and column means. All data in this project is synthetic (computer-generated). No real customer or bank is involved.

---

## 1. The idea in one minute (for a non-technical reader)

A bank receives data from many systems: customer lists, accounts, transactions, deposits and loans. That data arrives messy. Names are missing, the same customer appears twice, some amounts are negative by mistake, and some records point to customers that do not exist.

BankLake is a small data factory that does four jobs:

1. **Receives** the messy files exactly as they arrive.
2. **Cleans** them step by step and sets aside anything that looks wrong, with a reason written next to it.
3. **Protects privacy** by hiding names, emails, phone numbers and account numbers, and locking the real values in a separate safe.
4. **Produces reports** a bank manager can use to make decisions, and proves that no data went missing on the way.

Think of a restaurant kitchen. Ingredients arrive at the loading dock (raw files). They are put in the store room just as they came (Bronze). They are washed, sorted and the bad ones are thrown in a reject bin with a note (Silver and quarantine). Finally the chef plates them into dishes the customer actually orders (Gold reports). The valuables sit in a locked safe (the PII vault), and an inspector checks that every ingredient is accounted for (reconciliation).

---

## 2. Input: what goes in

Six files per run, in two formats.

| Source | Format | What it holds | Demo size |
|---|---|---|---|
| customer | CSV | People who bank here | 1,010 rows |
| account | CSV | Bank accounts owned by customers | 1,500 rows |
| transaction | JSON lines | Money moving in and out of accounts | about 10,000 rows |
| branch | CSV | Branch locations | 50 rows |
| deposit | CSV | Fixed deposits | 2,000 rows |
| loan | CSV | Loans | 500 rows |

The generator deliberately plants problems so the pipeline has something to catch:

| Problem planted | Example in the demo |
|---|---|
| Duplicate records | 10 duplicate customers, 70 duplicate transactions |
| Missing mandatory values | 8 customers with no name, 8 accounts with no ID |
| Impossible values | 80 negative or zero transaction amounts, invalid dates and balances |
| Broken references (orphans) | Accounts, transactions, deposits and loans pointing at records that do not exist |
| Corrupt lines | 3 malformed lines in the transaction file |

A second run can add new and changed records ("incremental"), to show that only new data is processed.

---

## 3. Process: what happens in between

| Step | Name | What it does | Plain meaning |
|---|---|---|---|
| 1 | Raw | Files as received | The loading dock |
| 2 | Bronze | Stores everything as text in a fast format (Parquet), adds bookkeeping columns, **hides personal details**, sets aside corrupt lines | Store room, with a privacy filter at the door |
| 3 | Silver | Fixes types (text to numbers and dates), trims and standardises text, keeps only the latest version of each record, checks every reference, merges new rows into old | Cleaned and checked |
| 3b | Quarantine | Holds every rejected row with the reason | Reject bin with a note |
| 4 | Gold | Joins and totals Silver into report tables | Dishes ready to serve |
| 5 | Checks | Compares every layer against the one before and names the first place anything goes wrong | The inspector |

What runs, in which order, how often it retries, and what is switched on are all stored in a settings table (the "metadata"), not hard-coded. If one step fails, everything that depends on it is marked BLOCKED and skipped, so bad data never flows onward.

---

## 4. Output: what comes out

1. **Five Gold report tables** for decisions (listed in section 6).
2. **A quarantine list** of every rejected row and why.
3. **A reconciliation report**: the latest run did 58 checks with 0 failed.
4. **Audit trails**: every pipeline run, every error, and every time someone viewed protected data.
5. **A dashboard** to explore all of the above (section 5).
6. **Converted SQL**: old-style bank queries rewritten for the cloud query engine, with proof that results match.
7. **A copy of the lake in AWS S3** (Sydney region): Bronze, Silver and Gold, with personal details already masked.

---

## 5. The dashboard: what the user sees

The dashboard is a web page that runs on your laptop. A sidebar menu switches pages, and a "Signed in as" box lets you pick a role. The role changes what you are allowed to see.

| Page | What a person does there |
|---|---|
| Overview | See row counts per layer and whether the last run succeeded |
| BI dashboards | Decision views built only from Gold: headline KPIs for executives, branches that need attention, customers to offer more products, customers needing credit review, fixed deposits maturing by month, the loan book, transaction trends |
| Pipeline flow | See the real step-by-step pipeline as a coloured diagram, then follow one table from Raw to Bronze to Silver to Quarantine to Gold with real row counts |
| Data explorer | Open any table in any layer: rows, search, column types, plain-language meaning and example values |
| Customers | Browse and filter customers with charts, and trace one customer through every layer |
| Ingestion & pipelines | Generate data, run layers, edit the settings |
| Data quality & reconciliation | Run the checks, deliberately break the data, watch the check catch it, then repair it |
| SQL migration | See the old query, the converted query, whether results match, and the refused one |
| Security & masking | Compare what an analyst sees with what an admin sees |
| Audit & monitoring | Alerts, freshness, pipeline runs by layer/entity/batch, ingestion and transformation audit, errors |
| Gold reports | View each of the five report tables with a chart (the BI dashboards page also lets you download action lists as CSV, with your role's masking applied) |

### Who sees what

| Role | Sees real values for |
|---|---|
| executive, analyst | Nothing. Always masked |
| branch_manager | Customer name |
| relationship_manager | Customer name, phone, email |
| compliance_officer, admin | Everything |

Every reveal, and every refused attempt, is written to an audit table.

### How masking looks

| Column | Original | Shown as |
|---|---|---|
| customer_name | Lajita Bal | L***** B** |
| email | lajita.bal1@example.com | l****@example.com |
| phone | 9330946985 | ******6985 |
| account_number | 123456789012 | ******9012 |

Each masked column also gets a `_token` column (a scrambled fingerprint). The token lets the system join records and look up the real value in the vault without ever exposing it in the lake.

---

## 6. Objects and attributes (data dictionary)

Types shown are the types after Silver cleaning. In Raw and Bronze everything is stored as text.

### 6.1 Customer

| Column | Type | Meaning | Example |
|---|---|---|---|
| customer_id | text, key | Unique customer ID | C000123 |
| customer_name | text, PII | Full name (masked from Bronze onward) | L***** B** |
| email | text, PII | Email address (masked) | l****@example.com |
| phone | text, PII | 10-digit mobile (masked) | ******6985 |
| city | text | City of residence | Chennai |
| segment | text | RETAIL, PREMIUM, SME or CORPORATE | RETAIL |
| updated_at | timestamp | Last change time, used to pick the latest version | 2025-03-29 10:37:00 |

Silver adds: `customer_name_token`, `email_token`, `phone_token`, `_batch_id`.

### 6.2 Account

| Column | Type | Meaning |
|---|---|---|
| account_id | text, key | Unique account ID |
| customer_id | text | Owner (must exist in customer) |
| branch_id | text | Branch holding the account |
| account_type | text | SAVINGS, CURRENT or SALARY |
| account_number | text, PII | 12-digit number (masked) |
| balance | decimal(18,2) | Current balance in rupees |
| opened_date | date | Date opened |
| updated_at | timestamp | Last change time |

Silver adds: `account_number_token`, `_batch_id`.

### 6.3 Transaction

| Column | Type | Meaning |
|---|---|---|
| transaction_id | text, key | Unique transaction ID |
| account_id | text | Account it belongs to (must exist) |
| transaction_date | date | Date of the transaction |
| amount | decimal(18,2) | Always positive; direction comes from txn_type |
| txn_type | text | CREDIT (money in) or DEBIT (money out) |

### 6.4 Branch

| Column | Type | Meaning |
|---|---|---|
| branch_id | text, key | Unique branch ID |
| branch_name | text | Branch name (fictional) |
| region | text | North, South, East, West or Central |

### 6.5 Deposit

| Column | Type | Meaning |
|---|---|---|
| deposit_id | text, key | Unique fixed-deposit ID |
| customer_id | text | Depositor (must exist) |
| deposit_amount | decimal(18,2) | Principal |
| maturity_date | date | When the deposit matures |

### 6.6 Loan

| Column | Type | Meaning |
|---|---|---|
| loan_id | text, key | Unique loan ID |
| customer_id | text | Borrower (must exist) |
| loan_amount | decimal(18,2) | Sanctioned amount |
| interest_rate | decimal(5,2) | Annual rate in percent |

### 6.7 What each layer adds

| Layer | Extra columns | Notes |
|---|---|---|
| Bronze | `_source_file`, `_batch_id`, `_ingest_ts`, `_load_type`, plus `<column>_token` for PII | Everything as text; PII already masked |
| Bronze quarantine | `_corrupt_record`, `_source_file` | Lines that could not even be read |
| Silver | `_batch_id` and token columns | Typed and deduplicated |
| Silver quarantine | `reject_reason`, `_run_id` | Rows rejected by a rule |

Rejection reasons: NULL_CUSTOMER_ID, NULL_CUSTOMER_NAME, NULL_ACCOUNT_ID, INVALID_BALANCE, INVALID_OPENED_DATE, ORPHAN_CUSTOMER_ID, NULL_TRANSACTION_ID, INVALID_TRANSACTION_DATE, INVALID_AMOUNT, NEGATIVE_AMOUNT, ORPHAN_ACCOUNT_ID, NULL_BRANCH_ID, and the deposit and loan equivalents.

### 6.8 Gold report tables

| Table | One row per | Columns |
|---|---|---|
| customer_360 | customer | customer_id, customer_name (masked), email (masked), phone (masked), customer_name_token, email_token, phone_token, customer_token, city, segment, n_accounts, total_balance, total_deposits, total_loans, n_transactions, total_txn_amount |
| branch_performance | branch | branch_id, branch_name, region, n_accounts, n_active_customers, total_balance, avg_balance, n_transactions, txn_amount |
| deposit_summary | segment and maturity month | segment, maturity_year, maturity_month, n_deposits, total_deposits, avg_deposit |
| loan_summary | segment | segment, n_loans, total_loan_amount, avg_interest_rate |
| transaction_summary | month and type | txn_month, txn_type, n_transactions, total_amount |

Decisions these support: which branches are strong or weak, which customers hold deposits but no loan (cross-sell), which customers have large loans against low balances (credit review), when cash leaves as deposits mature, and how transaction volume trends.

### 6.9 Settings and audit tables (SQLite)

| Table | Purpose | Main columns |
|---|---|---|
| pipeline_metadata | The list of pipelines and their settings | pipeline_id, source_table, target_table, source_layer, target_layer, load_strategy, primary_key, is_active, handler, max_retries |
| pipeline_dependencies | Which pipeline must finish first | pipeline_id, depends_on |
| masking_metadata | Which column gets which masking rule | entity, column_name, rule, is_active |
| role_permissions | Which role may unmask which column | role, entity, column_name |
| pipeline_watermark | The last batch each pipeline processed | pipeline_id, last_batch_id |
| ingested_files | Which files were already loaded | entity, file_name, batch_id, rows_read, rows_loaded, ts |
| pipeline_execution_log | One row per pipeline per run | run_id, pipeline_id, start_ts, end_ts, duration_s, rows_in, rows_out, rows_rejected, status, error_message, retry_count |
| pipeline_error_log | Errors raised | run_id, pipeline_id, ts, error |
| data_quality_results | Data quality findings | run_id, entity, check_name, status, value, detail, ts |
| reconciliation_results | Layer-to-layer comparisons | run_id, entity, stage, check_name, source_value, target_value, status, detail, ts |
| security_audit_log | Masking, unmasking and denials | ts, actor, action, target, detail |
| sql_migration_log | SQL conversion history | query_name, legacy_sql, converted_sql, applied, unsupported, status, match, detail |

### 6.10 The PII vault (separate file, never uploaded)

| Column | Meaning |
|---|---|
| entity | Which table the value came from |
| column_name | Which column |
| token | The scrambled fingerprint that links back |
| cipher | The real value, encrypted |

---

## 7. How we know it is right

- **Reconciliation:** raw rows equal Bronze plus corrupt lines; no keys lost between layers; no duplicates; no empty mandatory fields; money totals match to the paisa; Gold totals match Silver. Example: 1,010 raw customers = 992 in Silver + 8 rejected + 10 duplicates.
- **Privacy scan:** searches the whole lake for real names, emails, phones and account numbers. Expected result is zero, and it is zero.
- **Break and repair:** you can delete, duplicate or alter rows on purpose. The check names the first stage that went wrong, and a full refresh repairs it.
- **20 automated tests** cover all of the above and all pass.

---

## 8. Where it lives

- On the laptop: the `banklake` folder on the Desktop (code, data, dashboard, tests).
- In the cloud: an S3 bucket in the Sydney region holding the masked Bronze, Silver and Gold layers.
- Not in the cloud, on purpose: the PII vault, the settings database and the quarantine folder.

## 9. Still to build

Glue catalog and Athena queries, Apache Iceberg tables, scheduled automation (Step Functions, EventBridge, Lambda), performance benchmarks, and the two academic reports.
