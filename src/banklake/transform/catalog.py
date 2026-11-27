"""Procedure catalog. Each procedure is ONE Teradata SELECT that reads tables of the previous layer and describes a target table.

Teradata  : the SELECT only (no INSERT / UPDATE / DELETE / MERGE).
Athena    : the same SELECT converted by the sqlmigrate converter (see convert.py).
EMR       : a PySpark script that runs the SELECT with Spark SQL and does the writing (DELETE for the report date + append).

Layers and their source database:  silver_merge <- bronze,  silver_transform <- silver_merge,  gold <- silver_transform.
Every SELECT is filtered to one report date and returns it as a report_date column, so a run can be repeated safely.
"""
from ..schemas import ENTITIES, FLAG_COLUMNS, SOURCE_MASKING, SOURCE_ONLY, source_columns

LAYERS = ("silver_merge", "silver_transform", "gold")
LAYER_TITLE = {"silver_merge": "Silver Merge", "silver_transform": "Silver Transform", "gold": "Gold"}
SOURCE_DB = {"silver_merge": "bronze", "silver_transform": "silver_merge", "gold": "silver_transform"}
# the two choices on the page: Silver = both Silver steps
GROUPS = {"Silver": ("silver_merge", "silver_transform"), "Gold": ("gold",)}
RD = "DATE '{report_date}'"          # the report-date placeholder in the Teradata text

DDL = """
CREATE TABLE IF NOT EXISTS silver_transform_metadata(
  layer TEXT, database_name TEXT, target_table TEXT, source_table TEXT, source_database TEXT, phase TEXT,
  load_strategy TEXT, dependencies TEXT, script_path TEXT, is_pii TEXT DEFAULT 'N', pii_entity TEXT, pii_columns TEXT,
  is_active TEXT DEFAULT 'Y', execution_order INTEGER, PRIMARY KEY(layer, target_table));
CREATE TABLE IF NOT EXISTS gold_transform_metadata(
  database_name TEXT, target_table TEXT PRIMARY KEY, source_table TEXT, source_database TEXT, report_name TEXT, report_type TEXT,
  phase TEXT, load_strategy TEXT, dependencies TEXT, script_path TEXT, is_pii TEXT DEFAULT 'N', pii_entity TEXT, pii_columns TEXT,
  is_active TEXT DEFAULT 'Y', execution_order INTEGER);
CREATE TABLE IF NOT EXISTS gold_report_usage(
  target_table TEXT, report_name TEXT, report_type TEXT, PRIMARY KEY(target_table, report_name));
CREATE TABLE IF NOT EXISTS proc_artifacts(
  proc_id TEXT PRIMARY KEY, layer TEXT, athena_sql TEXT, emr_sql TEXT, script TEXT, script_path TEXT,
  athena_status TEXT, athena_notes TEXT, athena_ts TEXT, script_ts TEXT);
CREATE TABLE IF NOT EXISTS layer_audit_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT, layer TEXT, source_table_name TEXT, target_table_name TEXT,
  step_id INTEGER, status TEXT, timestamp TEXT, retry_count INTEGER, input_count INTEGER, output_count INTEGER,
  error_message TEXT, report_date TEXT);
"""



MASKED = {}
for _e, _c, _r in SOURCE_MASKING:
    MASKED.setdefault(_e, []).append(_c)
MONEY = {"daily_limit", "amount", "amount_due", "amount_paid", "penalty_amount", "market_value", "requested_amount", "annual_income", "risk_score", "ltv_ratio"}
RATE = {"buy_rate", "sell_rate", "mid_rate"}
INTS = {"credit_score", "score", "tenure_months", "resolution_days", "duration_sec", "enquiries_6m", "active_loans"}
CORE_TOKENS = {"customer": ["customer_name_token", "email_token", "phone_token"], "account": ["account_number_token"]}
ENTITY_LIST = list(ENTITIES) + list(SOURCE_ONLY)


def _cols(entity):
    """Bronze columns a Silver Merge procedure reads for one table."""
    if entity in ENTITIES:
        return [c for c in ENTITIES[entity]["columns"] if c not in ("source_system", "created_at")] + CORE_TOKENS.get(entity, [])
    return [c for c in source_columns(entity) if c != "source_system"] + [f"{c}_token" for c in MASKED.get(entity, [])]


def _key(entity):
    return ENTITIES[entity]["key"] if entity in ENTITIES else source_columns(entity)[0]


def _col_type(c):
    if c in MONEY:
        return "DECIMAL(18,2)"
    if c in RATE:
        return "DECIMAL(18,6)"
    if c in INTS:
        return "INTEGER"
    if c.endswith(("_date", "_on")):
        return "DATE"
    return None


# ---------------------------------------------------------------- Silver Merge: Bronze -> newest current record per key
def _merge_sql(entity):
    key, core = _key(entity), entity in ENTITIES
    cols = ",\n".join(f"        b.{c}" for c in _cols(entity))
    outc = ",\n".join(f"    m.{c}" for c in _cols(entity))
    op = "\n          AND UPPER(TRIM(COALESCE(b.op_type, 'I'))) IN ('I', 'U', 'D')" if core else ""
    deleted = "CASE WHEN UPPER(TRIM(m.op_type)) = 'D' THEN 1 ELSE 0 END" if core else "0"
    return f"""SEL
{outc},
    m.bronze_batch_id,
    {deleted} AS is_deleted,
    {RD} AS report_date
FROM (
    SEL
{cols},
        b."_batch_id" AS bronze_batch_id,
        ROW_NUMBER() OVER (PARTITION BY b.{key}
            ORDER BY b.updated_at DESC, b."_batch_id" DESC, b."_ingest_ts" DESC) AS rn
    FROM bronze.{entity} b
    WHERE b.{key} IS NOT NULL AND TRIM(b.{key}) <> ''{op}
          AND (CAST(SUBSTR(b.updated_at, 1, 10) AS DATE) IS NULL
               OR CAST(SUBSTR(b.updated_at, 1, 10) AS DATE) <= {RD})
) m
WHERE m.rn = 1"""


# ---------------------------------------------------------------- Silver Transform: Silver Merge -> clean tables, rejects, summaries
def _cur(a, entity):
    key = _key(entity)
    return f"{a}.report_date = {RD} AND {a}.is_deleted = 0 AND {a}.{key} IS NOT NULL AND TRIM({a}.{key}) <> ''"


NAME_OK = "c.customer_name IS NOT NULL AND TRIM(c.customer_name) <> ''"
VC = f"""vc AS (
    SEL TRIM(c.customer_id) AS customer_id
    FROM silver_merge.customer_merge c
    WHERE {_cur('c', 'customer')}
      AND {NAME_OK}
)"""
ACCOUNT_OK = "CAST(a.balance AS DECIMAL(18,2)) IS NOT NULL\n      AND CAST(a.opened_date AS DATE) IS NOT NULL"
VA = f"""va AS (
    SEL TRIM(a.account_id) AS account_id
    FROM silver_merge.account_merge a
    INNER JOIN vc ON TRIM(a.customer_id) = vc.customer_id
    WHERE {_cur('a', 'account')}
      AND {ACCOUNT_OK}
)"""

# entity -> (CTEs needed, LEFT JOIN text, [(reject reason, bad-row condition)])
CORE_RULES = {
    "customer": ((), "", [("NULL_CUSTOMER_NAME", "x.customer_name IS NULL OR TRIM(x.customer_name) = ''")]),
    "account": ((VC,), "LEFT JOIN vc ON TRIM(x.customer_id) = vc.customer_id",
                [("INVALID_BALANCE", "CAST(x.balance AS DECIMAL(18,2)) IS NULL"), ("INVALID_OPENED_DATE", "CAST(x.opened_date AS DATE) IS NULL"),
                 ("ORPHAN_CUSTOMER_ID", "vc.customer_id IS NULL")]),
    "transaction": ((VC, VA), "LEFT JOIN va ON TRIM(x.account_id) = va.account_id",
                    [("INVALID_TRANSACTION_DATE", "CAST(x.transaction_date AS DATE) IS NULL"),
                     ("INVALID_AMOUNT", "CAST(x.amount AS DECIMAL(18,2)) IS NULL"), ("NEGATIVE_AMOUNT", "CAST(x.amount AS DECIMAL(18,2)) <= 0"),
                     ("ORPHAN_ACCOUNT_ID", "va.account_id IS NULL")]),
    "branch": ((), "", []),
    "deposit": ((VC,), "LEFT JOIN vc ON TRIM(x.customer_id) = vc.customer_id",
                [("INVALID_DEPOSIT_AMOUNT", "CAST(x.deposit_amount AS DECIMAL(18,2)) IS NULL OR CAST(x.deposit_amount AS DECIMAL(18,2)) <= 0"),
                 ("ORPHAN_CUSTOMER_ID", "vc.customer_id IS NULL")]),
    "loan": ((VC,), "LEFT JOIN vc ON TRIM(x.customer_id) = vc.customer_id",
             [("INVALID_LOAN_AMOUNT", "CAST(x.loan_amount AS DECIMAL(18,2)) IS NULL OR CAST(x.loan_amount AS DECIMAL(18,2)) <= 0"),
              ("INVALID_INTEREST_RATE", "CAST(x.interest_rate AS DECIMAL(5,2)) IS NULL OR CAST(x.interest_rate AS DECIMAL(5,2)) <= 0 OR CAST(x.interest_rate AS DECIMAL(5,2)) > 100"),
              ("ORPHAN_CUSTOMER_ID", "vc.customer_id IS NULL")]),
}

CORE_SELECT = {
    "customer": """    TRIM(x.customer_id) AS customer_id,
    TRIM(x.customer_name) AS customer_name,
    TRIM(x.email) AS email,
    TRIM(x.phone) AS phone,
    x.customer_name_token, x.email_token, x.phone_token,
    TRIM(x.city) AS city,
    UPPER(TRIM(x.segment)) AS segment,
    CAST(x.updated_at AS TIMESTAMP) AS updated_at""",
    "account": """    TRIM(x.account_id) AS account_id,
    TRIM(x.customer_id) AS customer_id,
    TRIM(x.branch_id) AS branch_id,
    UPPER(TRIM(x.account_type)) AS account_type,
    TRIM(x.account_number) AS account_number,
    x.account_number_token,
    CAST(x.balance AS DECIMAL(18,2)) AS balance,
    CAST(x.opened_date AS DATE) AS opened_date,
    CAST(x.updated_at AS TIMESTAMP) AS updated_at""",
    "transaction": """    TRIM(x.transaction_id) AS transaction_id,
    TRIM(x.account_id) AS account_id,
    CAST(x.transaction_date AS DATE) AS transaction_date,
    CAST(x.amount AS DECIMAL(18,2)) AS amount,
    UPPER(TRIM(x.txn_type)) AS txn_type""",
    "branch": """    TRIM(x.branch_id) AS branch_id,
    TRIM(x.branch_name) AS branch_name,
    TRIM(x.region) AS region""",
    "deposit": """    TRIM(x.deposit_id) AS deposit_id,
    TRIM(x.customer_id) AS customer_id,
    CAST(x.deposit_amount AS DECIMAL(18,2)) AS deposit_amount,
    CAST(x.maturity_date AS DATE) AS maturity_date""",
    "loan": """    TRIM(x.loan_id) AS loan_id,
    TRIM(x.customer_id) AS customer_id,
    CAST(x.loan_amount AS DECIMAL(18,2)) AS loan_amount,
    CAST(x.interest_rate AS DECIMAL(5,2)) AS interest_rate""",
}


def _other_select(entity):
    flags, masked = set(FLAG_COLUMNS.get(entity, [])), set(MASKED.get(entity, []))
    out = []
    for c in source_columns(entity):
        if c == "source_system":
            continue
        t = _col_type(c)
        if c == "updated_at":
            out.append("CAST(x.updated_at AS TIMESTAMP) AS updated_at")
        elif c in masked:
            out.append(f"TRIM(x.{c}) AS {c},\n    x.{c}_token")
        elif t:
            out.append(f"CAST(x.{c} AS {t}) AS {c}")
        elif c in flags:
            out.append(f"UPPER(TRIM(x.{c})) AS {c}")
        else:
            out.append(f"TRIM(x.{c}) AS {c}")
    return "    " + ",\n    ".join(out)


def _rules(entity):
    if entity in CORE_RULES:
        return CORE_RULES[entity]
    bad = []
    for c in source_columns(entity):
        t = _col_type(c)
        if t:
            bad.append((f"INVALID_{c.upper()}", f"x.{c} IS NOT NULL AND TRIM(x.{c}) <> '' AND CAST(x.{c} AS {t}) IS NULL"))
    return ((), "", bad)


def _with(ctes):
    return ("WITH " + ",\n".join(ctes) + "\n") if ctes else ""


def _clean_sql(entity):
    ctes, join, bad = _rules(entity)
    sel = CORE_SELECT[entity] if entity in CORE_SELECT else _other_select(entity)
    where = "\n  AND ".join([_cur("x", entity)] + [f"NOT ({b})" if " OR " not in b else f"NOT ({b})" for _, b in bad])
    return f"""{_with(ctes)}SEL
{sel},
    {RD} AS report_date
FROM silver_merge.{entity}_merge x{(chr(10) + join) if join else ''}
WHERE {where}"""


def _rejects_sql(entity):
    ctes, join, bad = _rules(entity)
    key = _key(entity)
    if bad:
        case = "CASE " + " ".join(f"WHEN {b} THEN '{r}'" for r, b in bad) + " END"
        cond = "(" + ") OR (".join(b for _, b in bad) + ")"
    else:
        case, cond = "CAST(NULL AS VARCHAR(40))", "1 = 0"
    return f"""{_with(ctes)}SEL
    x.{key} AS record_key,
    x.updated_at,
    {case} AS reject_reason,
    x.bronze_batch_id,
    {RD} AS report_date
FROM silver_merge.{entity}_merge x{(chr(10) + join) if join else ''}
WHERE {_cur('x', entity)}
  AND ({cond})"""


def _derived():
    a, c, t = _cur("a", "account"), _cur("c", "customer"), _cur("t", "transaction")
    return {
        "customer_account_summary": ("Customer, account and balance band in one table", _with((VC,)) + f"""SEL
    TRIM(c.customer_id) AS customer_id,
    UPPER(TRIM(c.segment)) AS customer_type,
    TRIM(a.account_id) AS account_id,
    UPPER(TRIM(a.account_type)) AS account_type,
    CAST(a.balance AS DECIMAL(18,2)) AS ledger_balance,
    CASE
        WHEN CAST(a.balance AS DECIMAL(18,2)) >= 100000 THEN 'HIGH'
        WHEN CAST(a.balance AS DECIMAL(18,2)) >= 25000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS balance_category,
    {RD} AS report_date
FROM silver_merge.account_merge a
INNER JOIN silver_merge.customer_merge c ON TRIM(a.customer_id) = TRIM(c.customer_id) AND {c}
INNER JOIN vc ON TRIM(c.customer_id) = vc.customer_id
WHERE {a}
  AND {ACCOUNT_OK}"""),
        "account_balance": ("Account balance with transaction count and last transaction date", _with((VC, VA)) + f"""SEL
    TRIM(a.account_id) AS account_id,
    TRIM(a.customer_id) AS customer_id,
    CAST(a.balance AS DECIMAL(18,2)) AS ledger_balance,
    ZEROIFNULL(x.txn_count) AS txn_count,
    ZEROIFNULL(x.total_txn_amount) AS total_txn_amount,
    x.last_txn_date,
    {RD} AS report_date
FROM silver_merge.account_merge a
INNER JOIN va ON TRIM(a.account_id) = va.account_id
LEFT JOIN (SEL TRIM(t.account_id) AS account_id, COUNT(*) AS txn_count,
                  SUM(CAST(t.amount AS DECIMAL(18,2))) AS total_txn_amount,
                  MAX(CAST(t.transaction_date AS DATE)) AS last_txn_date
           FROM silver_merge.transaction_merge t
           WHERE {t} AND CAST(t.amount AS DECIMAL(18,2)) > 0
           GROUP BY TRIM(t.account_id)) x
       ON TRIM(a.account_id) = x.account_id
WHERE {a}"""),
        "customer_loan_exposure": ("Loans per customer", _with((VC,)) + f"""SEL
    TRIM(l.customer_id) AS customer_id,
    COUNT(*) AS n_loans,
    SUM(CAST(l.loan_amount AS DECIMAL(18,2))) AS total_loan_amount,
    MAX(CAST(l.interest_rate AS DECIMAL(5,2))) AS max_interest_rate,
    {RD} AS report_date
FROM silver_merge.loan_merge l
INNER JOIN vc ON TRIM(l.customer_id) = vc.customer_id
WHERE {_cur('l', 'loan')}
  AND CAST(l.loan_amount AS DECIMAL(18,2)) > 0
GROUP BY TRIM(l.customer_id)"""),
        "customer_deposit_exposure": ("Deposits per customer", _with((VC,)) + f"""SEL
    TRIM(d.customer_id) AS customer_id,
    COUNT(*) AS n_deposits,
    SUM(CAST(d.deposit_amount AS DECIMAL(18,2))) AS total_deposits,
    MIN(CAST(d.maturity_date AS DATE)) AS next_maturity_date,
    {RD} AS report_date
FROM silver_merge.deposit_merge d
INNER JOIN vc ON TRIM(d.customer_id) = vc.customer_id
WHERE {_cur('d', 'deposit')}
  AND CAST(d.deposit_amount AS DECIMAL(18,2)) > 0
GROUP BY TRIM(d.customer_id)"""),
        "branch_account_summary": ("Accounts and balances per branch", _with((VC,)) + f"""SEL
    TRIM(b.branch_id) AS branch_id,
    TRIM(b.branch_name) AS branch_name,
    TRIM(b.region) AS region,
    ZEROIFNULL(x.n_accounts) AS n_accounts,
    ZEROIFNULL(x.total_balance) AS total_balance,
    {RD} AS report_date
FROM silver_merge.branch_merge b
LEFT JOIN (SEL TRIM(a.branch_id) AS branch_id, COUNT(*) AS n_accounts, SUM(CAST(a.balance AS DECIMAL(18,2))) AS total_balance
           FROM silver_merge.account_merge a
           INNER JOIN vc ON TRIM(a.customer_id) = vc.customer_id
           WHERE {a} AND {ACCOUNT_OK}
           GROUP BY TRIM(a.branch_id)) x
       ON TRIM(b.branch_id) = x.branch_id
WHERE {_cur('b', 'branch')}"""),
        "daily_transaction_summary": ("Transactions per day and type", _with((VC, VA)) + f"""SEL
    CAST(t.transaction_date AS DATE) AS transaction_date,
    UPPER(TRIM(t.txn_type)) AS txn_type,
    COUNT(*) AS n_transactions,
    SUM(CAST(t.amount AS DECIMAL(18,2))) AS total_amount,
    {RD} AS report_date
FROM silver_merge.transaction_merge t
INNER JOIN va ON TRIM(t.account_id) = va.account_id
WHERE {t}
  AND CAST(t.transaction_date AS DATE) IS NOT NULL
  AND CAST(t.amount AS DECIMAL(18,2)) > 0
GROUP BY CAST(t.transaction_date AS DATE), UPPER(TRIM(t.txn_type))"""),
    }


def _gold_defs():
    import re
    R = "DATE '{report_date}'"
    d = {
        "gold_customer_overview": f"""SEL
    s.customer_type,
    COUNT(DISTINCT s.customer_id) AS total_customers,
    COUNT(DISTINCT s.account_id) AS total_accounts,
    SUM(s.ledger_balance) AS total_balance,
    s.report_date
FROM silver.customer_account_summary s
WHERE s.report_date = {R}
GROUP BY s.customer_type, s.report_date""",
        "gold_customer_360": f"""SEL
    c.customer_id,
    c.customer_name, c.email, c.phone,
    c.customer_name_token, c.email_token, c.phone_token,
    c.city, c.segment,
    ZEROIFNULL(a.n_accounts) AS n_accounts,
    ZEROIFNULL(a.total_balance) AS total_balance,
    ZEROIFNULL(d.total_deposits) AS total_deposits,
    ZEROIFNULL(l.total_loans) AS total_loans,
    ZEROIFNULL(t.n_transactions) AS n_transactions,
    ZEROIFNULL(t.total_txn_amount) AS total_txn_amount,
    c.report_date
FROM silver.customer_clean c
LEFT JOIN (SEL customer_id, COUNT(*) AS n_accounts, SUM(balance) AS total_balance
           FROM silver.account_clean WHERE report_date = {R} GROUP BY customer_id) a
       ON c.customer_id = a.customer_id
LEFT JOIN (SEL customer_id, SUM(deposit_amount) AS total_deposits
           FROM silver.deposit_clean WHERE report_date = {R} GROUP BY customer_id) d
       ON c.customer_id = d.customer_id
LEFT JOIN (SEL customer_id, SUM(loan_amount) AS total_loans
           FROM silver.loan_clean WHERE report_date = {R} GROUP BY customer_id) l
       ON c.customer_id = l.customer_id
LEFT JOIN (SEL x.customer_id, COUNT(*) AS n_transactions, SUM(tr.amount) AS total_txn_amount
           FROM silver.transaction_clean tr
           INNER JOIN silver.account_clean x ON tr.account_id = x.account_id AND x.report_date = {R}
           WHERE tr.report_date = {R} GROUP BY x.customer_id) t
       ON c.customer_id = t.customer_id
WHERE c.report_date = {R}""",
        "gold_branch_performance": f"""SEL
    b.branch_id, b.branch_name, b.region,
    ZEROIFNULL(x.n_accounts) AS n_accounts,
    ZEROIFNULL(x.n_customers) AS n_active_customers,
    ZEROIFNULL(x.total_balance) AS total_balance,
    x.avg_balance,
    ZEROIFNULL(y.n_txn) AS n_transactions,
    ZEROIFNULL(y.txn_amount) AS txn_amount,
    b.report_date
FROM silver.branch_clean b
LEFT JOIN (SEL branch_id, COUNT(*) AS n_accounts, COUNT(DISTINCT customer_id) AS n_customers,
                  SUM(balance) AS total_balance, CAST(AVG(balance) AS DECIMAL(18,2)) AS avg_balance
           FROM silver.account_clean WHERE report_date = {R} GROUP BY branch_id) x
       ON b.branch_id = x.branch_id
LEFT JOIN (SEL a.branch_id, COUNT(*) AS n_txn, SUM(t.amount) AS txn_amount
           FROM silver.transaction_clean t
           INNER JOIN silver.account_clean a ON t.account_id = a.account_id AND a.report_date = {R}
           WHERE t.report_date = {R} GROUP BY a.branch_id) y
       ON b.branch_id = y.branch_id
WHERE b.report_date = {R}""",
        "gold_deposit_summary": f"""SEL
    c.segment,
    EXTRACT(YEAR FROM d.maturity_date) AS maturity_year,
    TO_CHAR(d.maturity_date, 'YYYY-MM') AS maturity_month,
    COUNT(*) AS n_deposits,
    SUM(d.deposit_amount) AS total_deposits,
    CAST(AVG(d.deposit_amount) AS DECIMAL(18,2)) AS avg_deposit,
    d.report_date
FROM silver.deposit_clean d
INNER JOIN silver.customer_clean c ON d.customer_id = c.customer_id AND c.report_date = {R}
WHERE d.report_date = {R}
GROUP BY c.segment, EXTRACT(YEAR FROM d.maturity_date), TO_CHAR(d.maturity_date, 'YYYY-MM'), d.report_date""",
        "gold_loan_summary": f"""SEL
    c.segment,
    COUNT(*) AS n_loans,
    SUM(l.loan_amount) AS total_loan_amount,
    CAST(AVG(l.interest_rate) AS DECIMAL(5,2)) AS avg_interest_rate,
    l.report_date
FROM silver.loan_clean l
INNER JOIN silver.customer_clean c ON l.customer_id = c.customer_id AND c.report_date = {R}
WHERE l.report_date = {R}
GROUP BY c.segment, l.report_date""",
        "gold_transaction_summary": f"""SEL
    TO_CHAR(t.transaction_date, 'YYYY-MM') AS txn_month,
    t.txn_type,
    COUNT(*) AS n_transactions,
    SUM(t.amount) AS total_amount,
    t.report_date
FROM silver.transaction_clean t
WHERE t.report_date = {R}
GROUP BY TO_CHAR(t.transaction_date, 'YYYY-MM'), t.txn_type, t.report_date""",
        "gold_card_portfolio": f"""SEL
    c.card_type, c.network, c.card_status,
    COUNT(*) AS n_cards,
    CAST(AVG(c.daily_limit) AS DECIMAL(18,2)) AS avg_daily_limit,
    c.report_date
FROM silver.card_clean c
WHERE c.report_date = {R}
GROUP BY c.card_type, c.network, c.card_status, c.report_date""",
        "gold_fraud_alert_summary": f"""SEL
    f.rule_code, f.alert_status,
    COUNT(*) AS n_alerts,
    CAST(AVG(f.risk_score) AS DECIMAL(18,2)) AS avg_risk_score,
    f.report_date
FROM silver.fraud_alert_clean f
WHERE f.report_date = {R}
GROUP BY f.rule_code, f.alert_status, f.report_date""",
        "gold_complaint_summary": f"""SEL
    c.category, c.severity, c.complaint_status,
    COUNT(*) AS n_complaints,
    CAST(AVG(c.resolution_days) AS DECIMAL(10,1)) AS avg_resolution_days,
    c.report_date
FROM silver.customer_complaint_clean c
WHERE c.report_date = {R}
GROUP BY c.category, c.severity, c.complaint_status, c.report_date""",
        "gold_fx_rate_summary": f"""SEL
    x.currency_pair,
    COUNT(*) AS n_rates,
    CAST(AVG(x.mid_rate) AS DECIMAL(18,6)) AS avg_mid_rate,
    MIN(x.buy_rate) AS min_buy_rate,
    MAX(x.sell_rate) AS max_sell_rate,
    MAX(x.rate_date) AS latest_rate_date,
    x.report_date
FROM silver.fx_rate_clean x
WHERE x.report_date = {R}
GROUP BY x.currency_pair, x.report_date""",
        "gold_loan_repayment_summary": f"""SEL
    r.repayment_status,
    COUNT(*) AS n_repayments,
    SUM(r.amount_due) AS total_due,
    SUM(r.amount_paid) AS total_paid,
    SUM(r.penalty_amount) AS total_penalty,
    r.report_date
FROM silver.loan_repayment_clean r
WHERE r.report_date = {R}
GROUP BY r.repayment_status, r.report_date""",
        "gold_kyc_status_summary": f"""SEL
    k.verification_status, c.segment,
    COUNT(DISTINCT k.customer_id) AS n_customers,
    k.report_date
FROM silver.customer_kyc_clean k
INNER JOIN silver.customer_clean c ON k.customer_id = c.customer_id AND c.report_date = {R}
WHERE k.report_date = {R}
GROUP BY k.verification_status, c.segment, k.report_date""",
        "gold_credit_score_summary": f"""SEL
    s.bureau, s.score_band,
    COUNT(*) AS n_scores,
    CAST(AVG(s.score) AS DECIMAL(10,1)) AS avg_score,
    s.report_date
FROM silver.credit_bureau_score_clean s
WHERE s.report_date = {R}
GROUP BY s.bureau, s.score_band, s.report_date""",
        "gold_campaign_response_summary": f"""SEL
    r.campaign_code, r.response,
    COUNT(*) AS n_responses,
    r.report_date
FROM silver.campaign_response_clean r
WHERE r.report_date = {R}
GROUP BY r.campaign_code, r.response, r.report_date""",
    }
    return {k: re.sub(r"\bsilver\.(\w+)", r"silver_transform.\1", v) for k, v in d.items()}


# gold table -> (primary report, type, PII entity, PII columns)
GOLD_INFO = {
    "gold_customer_overview": ("Customer Overview Dashboard", "Dashboard", None, ()),
    "gold_customer_360": ("Customer 360 View", "BI", "customer", ("customer_name", "email", "phone")),
    "gold_branch_performance": ("Branch Performance Report", "Report", None, ()),
    "gold_deposit_summary": ("Deposit Maturity Report", "Report", None, ()),
    "gold_loan_summary": ("Loan Portfolio Dashboard", "Dashboard", None, ()),
    "gold_transaction_summary": ("Monthly Transaction Trends", "BI", None, ()),
    "gold_card_portfolio": ("Card Portfolio Report", "Report", None, ()),
    "gold_fraud_alert_summary": ("Fraud Monitoring Dashboard", "Dashboard", None, ()),
    "gold_complaint_summary": ("Complaints Report", "Report", None, ()),
    "gold_fx_rate_summary": ("Treasury FX Dashboard", "Dashboard", None, ()),
    "gold_loan_repayment_summary": ("Loan Collections Report", "Report", None, ()),
    "gold_kyc_status_summary": ("KYC Compliance Report", "Regulatory", None, ()),
    "gold_credit_score_summary": ("Credit Risk Dashboard", "Dashboard", None, ()),
    "gold_campaign_response_summary": ("Marketing Campaign Report", "BI", None, ()),
}
REPORT_USAGE = [
    ("gold_customer_overview", "Customer Overview Dashboard", "Dashboard"),
    ("gold_customer_overview", "Executive Summary", "Dashboard"),
    ("gold_customer_360", "Customer 360 View", "BI"),
    ("gold_customer_360", "Relationship Manager Report", "Report"),
    ("gold_branch_performance", "Branch Performance Report", "Report"),
    ("gold_branch_performance", "Regional Branch Scorecard", "BI"),
    ("gold_deposit_summary", "Deposit Maturity Report", "Report"),
    ("gold_deposit_summary", "Executive Summary", "Dashboard"),
    ("gold_loan_summary", "Loan Portfolio Dashboard", "Dashboard"),
    ("gold_loan_summary", "Executive Summary", "Dashboard"),
    ("gold_transaction_summary", "Monthly Transaction Trends", "BI"),
    ("gold_transaction_summary", "Regional Branch Scorecard", "BI"),
    ("gold_card_portfolio", "Card Portfolio Report", "Report"),
    ("gold_fraud_alert_summary", "Fraud Monitoring Dashboard", "Dashboard"),
    ("gold_complaint_summary", "Complaints Report", "Report"),
    ("gold_complaint_summary", "Regulatory Complaints Return", "Regulatory"),
    ("gold_fx_rate_summary", "Treasury FX Dashboard", "Dashboard"),
    ("gold_loan_repayment_summary", "Loan Collections Report", "Report"),
    ("gold_loan_repayment_summary", "Credit Risk Dashboard", "Dashboard"),
    ("gold_kyc_status_summary", "KYC Compliance Report", "Regulatory"),
    ("gold_credit_score_summary", "Credit Risk Dashboard", "Dashboard"),
    ("gold_campaign_response_summary", "Marketing Campaign Report", "BI"),
]


def _sources(sql, db):
    import re
    seen = []
    for m in re.finditer(rf"\b{db}\.(\w+)", sql):
        if m.group(1) not in seen:
            seen.append(m.group(1))
    return seen


def _build():
    procs = []
    for e in ENTITY_LIST:
        procs.append(dict(layer="silver_merge", target=f"{e}_merge", teradata=_merge_sql(e), desc=f"{e}: newest record per key from Bronze, deletes flagged"))
    for e in ENTITY_LIST:
        procs.append(dict(layer="silver_transform", target=f"{e}_clean", teradata=_clean_sql(e), desc=f"{e}: trimmed, typed and checked"))
    for e in ENTITY_LIST:
        procs.append(dict(layer="silver_transform", target=f"{e}_rejects", teradata=_rejects_sql(e), desc=f"{e}: records turned away, with the reason"))
    for t, (desc, sql) in _derived().items():
        procs.append(dict(layer="silver_transform", target=t, teradata=sql, desc=desc))
    for t, sql in _gold_defs().items():
        procs.append(dict(layer="gold", target=t, teradata=sql, desc=f"Feeds: {GOLD_INFO[t][0]}"))
    for i, p in enumerate(procs, 1):
        p["sources"] = _sources(p["teradata"], SOURCE_DB[p["layer"]])
        p["order"] = i
        p["proc_id"] = f"{p['layer']}.{p['target']}"
        p["script_name"] = f"load_{p['target']}"
        p["source_db"] = SOURCE_DB[p["layer"]]
    return procs


PROCS = _build()
BY_ID = {p["proc_id"]: p for p in PROCS}
BY_TARGET = {(p["layer"], p["target"]): p for p in PROCS}


def upstream(pid):
    """Procedures that load the source tables of this one, and theirs in turn, in load order (Bronze has none)."""
    out, todo = {}, [BY_ID[pid]]
    while todo:
        p = todo.pop()
        for s in p["sources"]:
            q = BY_TARGET.get((p["source_db"], s))
            if q and q["proc_id"] not in out:
                out[q["proc_id"]] = q
                todo.append(q)
    return sorted(out.values(), key=lambda q: q["order"])


def procs_for(layer):
    return [p for p in PROCS if p["layer"] == layer]


def ensure(c):
    """Create the metadata tables and register every procedure (idempotent). Takes an open sqlite connection."""
    c.executescript(DDL)
    c.execute("DELETE FROM silver_transform_metadata")
    c.execute("DELETE FROM gold_transform_metadata")
    for p in PROCS:
        path = f"emr_scripts/{p['layer']}/{p['script_name']}.py"
        deps = ",".join(f"{p['source_db']}.{s}" for s in p["sources"])
        if p["layer"] == "gold":
            rep, typ, ent, cols = GOLD_INFO[p["target"]]
            c.execute("INSERT OR REPLACE INTO gold_transform_metadata(database_name,target_table,source_table,source_database,report_name,report_type,"
                      "phase,load_strategy,dependencies,script_path,is_pii,pii_entity,pii_columns,is_active,execution_order) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      ("gold", p["target"], ",".join(p["sources"]), p["source_db"], rep, typ, "GOLD", "REPORT_DATE_REPLACE", deps, path,
                       "Y" if ent else "N", ent, ",".join(cols), "Y", p["order"]))
        else:
            c.execute("INSERT OR REPLACE INTO silver_transform_metadata(layer,database_name,target_table,source_table,source_database,phase,load_strategy,"
                      "dependencies,script_path,is_pii,pii_entity,pii_columns,is_active,execution_order) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (p["layer"], p["layer"], p["target"], ",".join(p["sources"]), p["source_db"], "SILVER", "REPORT_DATE_REPLACE", deps, path,
                       "N", None, "", "Y", p["order"]))
    c.execute("DELETE FROM gold_report_usage")
    c.executemany("INSERT OR REPLACE INTO gold_report_usage(target_table,report_name,report_type) VALUES(?,?,?)", REPORT_USAGE)
