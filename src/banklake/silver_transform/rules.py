"""Business rules and foreign-key checks. Each rule list is ordered: the FIRST matching rule becomes the
reject_reason. Only child-to-parent orphans are errors; a parent with no children (a customer with no loan)
is valid and is not checked here.

Documented rules
  customer     id and name present
  account      id present; balance is a number; opened_date is a real date; customer exists
  transaction  id present; date is a real date; amount is a number and > 0; account exists
  branch       id present
  deposit      id present; amount is a number and > 0; customer exists
  loan         id present; amount is a number and > 0; interest rate in (0, 100]; customer exists
"""
from pyspark.sql import functions as F

from ..common import layer_path
from .cleaning import blank


def parent_ids(spark, parent: str, key: str, flag: str):
    """Distinct parent keys from the Silver output of the parent entity, for the orphan (FK) join."""
    return (spark.read.parquet(str(layer_path("silver", parent)))
            .select(F.col(key).alias("_ref_" + key)).distinct().withColumn(flag, F.lit(1)))


def classify(df, rules):
    """Adds reject_reason (NULL = valid). First matching rule wins."""
    expr = None
    for reason, cond in rules:
        expr = F.when(cond, reason) if expr is None else expr.when(cond, reason)
    return df.withColumn("reject_reason", expr)


def with_parent(spark, df, parent, fk, flag):
    """LEFT JOIN to the parent's keys: rows with no match get flag NULL (checked by the rules)."""
    return df.join(F.broadcast(parent_ids(spark, parent, fk, flag)), df[fk] == F.col("_ref_" + fk), "left")


def customer(spark, df):
    return classify(df, [("NULL_CUSTOMER_ID", blank("customer_id")), ("NULL_CUSTOMER_NAME", blank("customer_name"))])


def account(spark, df):
    a = with_parent(spark, df, "customer", "customer_id", "_c_ok")
    return classify(a, [("NULL_ACCOUNT_ID", blank("account_id")),
                        ("INVALID_BALANCE", F.col("balance").isNull()),
                        ("INVALID_OPENED_DATE", F.col("opened_date").isNull()),
                        ("ORPHAN_CUSTOMER_ID", F.col("_c_ok").isNull())]).drop("_ref_customer_id", "_c_ok")


def transaction(spark, df):
    x = with_parent(spark, df, "account", "account_id", "_a_ok")
    return classify(x, [("NULL_TRANSACTION_ID", blank("transaction_id")),
                        ("INVALID_TRANSACTION_DATE", F.col("transaction_date").isNull()),
                        ("INVALID_AMOUNT", F.col("amount").isNull()),
                        ("NEGATIVE_AMOUNT", F.col("amount") <= 0),
                        ("ORPHAN_ACCOUNT_ID", F.col("_a_ok").isNull())]).drop("_ref_account_id", "_a_ok")


def branch(spark, df):
    return classify(df, [("NULL_BRANCH_ID", blank("branch_id"))])


def _customer_child(spark, df, key, amount_col, extra=()):
    d = with_parent(spark, df, "customer", "customer_id", "_c_ok")
    return classify(d, [(f"NULL_{key.upper()}", blank(key)),
                        (f"INVALID_{amount_col.upper()}", F.col(amount_col).isNull() | (F.col(amount_col) <= 0)),
                        *extra,
                        ("ORPHAN_CUSTOMER_ID", F.col("_c_ok").isNull())]).drop("_ref_customer_id", "_c_ok")


def deposit(spark, df):
    return _customer_child(spark, df, "deposit_id", "deposit_amount")


def loan(spark, df):
    rate = F.col("interest_rate")
    return _customer_child(spark, df, "loan_id", "loan_amount",
                           [("INVALID_INTEREST_RATE", rate.isNull() | (rate <= 0) | (rate > 100))])


RULES = {"customer": customer, "account": account, "transaction": transaction,
         "branch": branch, "deposit": deposit, "loan": loan}
