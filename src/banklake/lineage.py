"""Gold lineage: which Silver columns feed each Gold column, and how.

The table-level lineage is read live from pipeline_dependencies. The column-level lineage below is written by hand
from gold.py and is checked by tests/test_lineage.py against the real Gold and Silver tables, so it cannot silently
drift: a renamed or added column makes the test fail.
"""
from . import meta

# gold table -> (grain, {column: (logic, [silver entity.column, ...])})
GOLD_LINEAGE = {
    "customer_360": ("one row per customer", {
        "customer_id": ("copied", ["customer.customer_id"]),
        "customer_name": ("unmasked from the vault at Gold (masked in Bronze, Silver Merge and Silver Transform)", ["customer.customer_name"]),
        "email": ("unmasked from the vault at Gold", ["customer.email"]),
        "phone": ("unmasked from the vault at Gold", ["customer.phone"]),
        "customer_name_token": ("copied; key into the PII vault", ["customer.customer_name_token"]),
        "email_token": ("copied; key into the PII vault", ["customer.email_token"]),
        "phone_token": ("copied; key into the PII vault", ["customer.phone_token"]),
        "city": ("copied", ["customer.city"]),
        "segment": ("copied", ["customer.segment"]),
        "n_accounts": ("COUNT of the customer's accounts (0 if none)", ["account.account_id", "account.customer_id"]),
        "total_balance": ("SUM(balance) over the customer's accounts (0 if none)", ["account.balance", "account.customer_id"]),
        "total_deposits": ("SUM(deposit_amount) over the customer's deposits (0 if none)", ["deposit.deposit_amount", "deposit.customer_id"]),
        "total_loans": ("SUM(loan_amount) over the customer's loans (0 if none)", ["loan.loan_amount", "loan.customer_id"]),
        "n_transactions": ("COUNT of transactions on the customer's accounts (transaction JOIN account)", ["transaction.transaction_id", "transaction.account_id", "account.account_id", "account.customer_id"]),
        "total_txn_amount": ("SUM(amount) of those transactions (0 if none)", ["transaction.amount", "transaction.account_id", "account.customer_id"]),
        "customer_token": ("salted SHA-256 pseudonym of customer_id", ["customer.customer_id"]),
    }),
    "branch_performance": ("one row per branch", {
        "branch_id": ("copied", ["branch.branch_id"]),
        "branch_name": ("copied", ["branch.branch_name"]),
        "region": ("copied", ["branch.region"]),
        "n_accounts": ("COUNT of accounts at the branch (0 if none)", ["account.account_id", "account.branch_id"]),
        "n_active_customers": ("COUNT DISTINCT customer_id over the branch's accounts", ["account.customer_id", "account.branch_id"]),
        "total_balance": ("SUM(balance) at the branch (0 if none)", ["account.balance", "account.branch_id"]),
        "avg_balance": ("AVG(balance) as DECIMAL(18,2); NULL if no accounts", ["account.balance", "account.branch_id"]),
        "n_transactions": ("COUNT of transactions on the branch's accounts", ["transaction.transaction_id", "transaction.account_id", "account.branch_id"]),
        "txn_amount": ("SUM(amount) of those transactions (0 if none)", ["transaction.amount", "transaction.account_id", "account.branch_id"]),
    }),
    "deposit_summary": ("one row per customer segment and maturity month", {
        "segment": ("customer's segment (deposit JOIN customer)", ["customer.segment", "customer.customer_id", "deposit.customer_id"]),
        "maturity_year": ("YEAR(maturity_date)", ["deposit.maturity_date"]),
        "maturity_month": ("maturity_date formatted yyyy-MM", ["deposit.maturity_date"]),
        "n_deposits": ("COUNT of deposits in the group", ["deposit.deposit_id"]),
        "total_deposits": ("SUM(deposit_amount)", ["deposit.deposit_amount"]),
        "avg_deposit": ("AVG(deposit_amount) as DECIMAL(18,2)", ["deposit.deposit_amount"]),
    }),
    "loan_summary": ("one row per customer segment", {
        "segment": ("customer's segment (loan JOIN customer)", ["customer.segment", "customer.customer_id", "loan.customer_id"]),
        "n_loans": ("COUNT of loans in the segment", ["loan.loan_id"]),
        "total_loan_amount": ("SUM(loan_amount)", ["loan.loan_amount"]),
        "avg_interest_rate": ("AVG(interest_rate) as DECIMAL(5,2)", ["loan.interest_rate"]),
    }),
    "transaction_summary": ("one row per month and transaction type", {
        "txn_month": ("transaction_date formatted yyyy-MM", ["transaction.transaction_date"]),
        "txn_type": ("copied (grouping key)", ["transaction.txn_type"]),
        "n_transactions": ("COUNT of transactions in the group", ["transaction.transaction_id"]),
        "total_amount": ("SUM(amount)", ["transaction.amount"]),
    }),
}


def table_lineage() -> dict:
    """gold table -> the Silver entities its pipeline depends on, read from pipeline_dependencies (the live metadata)."""
    out = {}
    for d in meta.query("SELECT pipeline_id, depends_on FROM pipeline_dependencies WHERE pipeline_id LIKE 'gold_%'"):
        out.setdefault(d["pipeline_id"][5:], []).append(d["depends_on"].replace("silver_", "", 1))
    return {k: sorted(v) for k, v in sorted(out.items())}


def column_rows(table: str) -> list[dict]:
    grain, cols = GOLD_LINEAGE[table]
    return [dict(gold_column=c, logic=logic, silver_sources=", ".join(src)) for c, (logic, src) in cols.items()]


def to_markdown() -> str:
    lines = ["# Gold lineage", "",
             "Silver to Gold, table by table and column by column. Table-level dependencies come from the pipeline metadata; "
             "column-level lineage is checked against the real tables by `tests/test_lineage.py`.", "",
             "Full path of every Gold column: raw file -> Bronze -> Silver Merge (CDC) -> Silver Transform (clean, validate; still masked) -> Silver -> Gold (customer PII unmasked).", ""]
    deps = table_lineage()
    for t, (grain, cols) in GOLD_LINEAGE.items():
        lines += [f"## gold.{t}", f"Grain: {grain}. Built from Silver: {', '.join(deps.get(t, []))}.", "",
                  "| Gold column | How it is computed | Silver sources |", "|---|---|---|"]
        lines += [f"| {c} | {logic} | {', '.join(src)} |" for c, (logic, src) in cols.items()]
        lines.append("")
    return "\n".join(lines)
