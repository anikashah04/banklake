"""Gold layer: business-ready reporting tables built with Spark SQL over Silver."""
from pyspark.sql import functions as F

from . import vault
from .common import layer_path
from .masking import tokenize


def _views(spark, *entities):
    for e in entities:
        spark.read.parquet(str(layer_path("silver", e))).createOrReplaceTempView(f"s_{e}")


def _write(df, name, rows_in):
    df = df.cache()
    df.write.mode("overwrite").parquet(str(layer_path("gold", name)))
    return dict(rows_in=rows_in, rows_out=df.count(), rows_rejected=0)


def run_customer_360(spark, m, ctx):
    _views(spark, "customer", "account", "transaction", "deposit", "loan")
    df = spark.sql("""
      SELECT c.customer_id, c.customer_name, c.email, c.phone, c.customer_name_token, c.email_token, c.phone_token, c.city, c.segment,
             COALESCE(a.n_accounts, 0) AS n_accounts, COALESCE(a.total_balance, 0) AS total_balance,
             COALESCE(d.total_deposits, 0) AS total_deposits, COALESCE(l.total_loans, 0) AS total_loans,
             COALESCE(t.n_transactions, 0) AS n_transactions, COALESCE(t.total_txn_amount, 0) AS total_txn_amount
      FROM s_customer c
      LEFT JOIN (SELECT customer_id, COUNT(*) n_accounts, SUM(balance) total_balance
                 FROM s_account GROUP BY customer_id) a ON c.customer_id = a.customer_id
      LEFT JOIN (SELECT customer_id, SUM(deposit_amount) total_deposits
                 FROM s_deposit GROUP BY customer_id) d ON c.customer_id = d.customer_id
      LEFT JOIN (SELECT customer_id, SUM(loan_amount) total_loans
                 FROM s_loan GROUP BY customer_id) l ON c.customer_id = l.customer_id
      LEFT JOIN (SELECT a.customer_id, COUNT(*) n_transactions, SUM(t.amount) total_txn_amount
                 FROM s_transaction t JOIN s_account a ON t.account_id = a.account_id
                 GROUP BY a.customer_id) t ON c.customer_id = t.customer_id""")
    df = tokenize(df, "customer_id", "customer_token")
    # Bronze, Silver Merge and Silver Transform hold masked PII. Gold is the first unmasked layer: the pipeline reads
    # the real values back from the vault (audited UNMASK_AT_GOLD). The *_token columns stay for traceability.
    df = vault.unmask_for_gold(spark, df, "customer", ["customer_name", "email", "phone"])
    return _write(df, "customer_360", spark.table("s_customer").count())


def run_branch_performance(spark, m, ctx):
    _views(spark, "branch", "account", "transaction")
    df = spark.sql("""
      SELECT b.branch_id, b.branch_name, b.region,
             COALESCE(x.n_accounts, 0) AS n_accounts, COALESCE(x.n_customers, 0) AS n_active_customers,
             COALESCE(x.total_balance, 0) AS total_balance, x.avg_balance,
             COALESCE(y.n_txn, 0) AS n_transactions, COALESCE(y.txn_amount, 0) AS txn_amount
      FROM s_branch b
      LEFT JOIN (SELECT branch_id, COUNT(*) n_accounts, COUNT(DISTINCT customer_id) n_customers,
                        SUM(balance) total_balance, CAST(AVG(balance) AS DECIMAL(18,2)) avg_balance
                 FROM s_account GROUP BY branch_id) x ON b.branch_id = x.branch_id
      LEFT JOIN (SELECT a.branch_id, COUNT(*) n_txn, SUM(t.amount) txn_amount
                 FROM s_transaction t JOIN s_account a ON t.account_id = a.account_id
                 GROUP BY a.branch_id) y ON b.branch_id = y.branch_id""")
    return _write(df, "branch_performance", spark.table("s_branch").count())


def run_deposit_summary(spark, m, ctx):
    _views(spark, "deposit", "customer")
    df = spark.sql("""
      SELECT c.segment, YEAR(d.maturity_date) AS maturity_year, DATE_FORMAT(d.maturity_date, 'yyyy-MM') AS maturity_month,
             COUNT(*) AS n_deposits,
             SUM(d.deposit_amount) AS total_deposits, CAST(AVG(d.deposit_amount) AS DECIMAL(18,2)) AS avg_deposit
      FROM s_deposit d JOIN s_customer c ON d.customer_id = c.customer_id
      GROUP BY c.segment, YEAR(d.maturity_date), DATE_FORMAT(d.maturity_date, 'yyyy-MM')""")
    return _write(df, "deposit_summary", spark.table("s_deposit").count())


def run_loan_summary(spark, m, ctx):
    _views(spark, "loan", "customer")
    df = spark.sql("""
      SELECT c.segment, COUNT(*) AS n_loans, SUM(l.loan_amount) AS total_loan_amount,
             CAST(AVG(l.interest_rate) AS DECIMAL(5,2)) AS avg_interest_rate
      FROM s_loan l JOIN s_customer c ON l.customer_id = c.customer_id GROUP BY c.segment""")
    return _write(df, "loan_summary", spark.table("s_loan").count())


def run_transaction_summary(spark, m, ctx):
    _views(spark, "transaction")
    df = spark.sql("""
      SELECT DATE_FORMAT(transaction_date, 'yyyy-MM') AS txn_month, txn_type,
             COUNT(*) AS n_transactions, SUM(amount) AS total_amount
      FROM s_transaction GROUP BY DATE_FORMAT(transaction_date, 'yyyy-MM'), txn_type""")
    return _write(df, "transaction_summary", spark.table("s_transaction").count())
