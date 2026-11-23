"""Type conversion, null handling and text standardisation for each entity (no business rules here)."""
from pyspark.sql import functions as F


def _tok(df):
    """PII is masked at Bronze; the {col}_token columns must survive cleaning so Silver Transform does not re-mask."""
    return [c for c in df.columns if c.endswith("_token")]


def blank(c):
    """True for NULL or whitespace-only text."""
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def customer(df):
    return df.select(
        F.trim("customer_id").alias("customer_id"), F.trim("customer_name").alias("customer_name"),
        F.trim("email").alias("email"), F.trim("phone").alias("phone"),
        F.initcap(F.trim("city")).alias("city"), F.upper(F.trim("segment")).alias("segment"),
        F.to_timestamp("updated_at").alias("updated_at"), "_batch_id", *_tok(df))


def account(df):
    return df.select(
        F.trim("account_id").alias("account_id"), F.trim("customer_id").alias("customer_id"),
        F.trim("branch_id").alias("branch_id"), F.upper(F.trim("account_type")).alias("account_type"),
        F.trim("account_number").alias("account_number"),
        F.col("balance").cast("decimal(18,2)").alias("balance"),
        F.to_date("opened_date").alias("opened_date"),
        F.to_timestamp("updated_at").alias("updated_at"), "_batch_id", *_tok(df))


def transaction(df):
    return df.select(
        F.trim("transaction_id").alias("transaction_id"), F.trim("account_id").alias("account_id"),
        F.to_date("transaction_date").alias("transaction_date"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim("txn_type")).alias("txn_type"), "_batch_id", *_tok(df))


def branch(df):
    return df.select(F.trim("branch_id").alias("branch_id"), F.initcap(F.trim("branch_name")).alias("branch_name"),
                     F.initcap(F.trim("region")).alias("region"), "_batch_id", *_tok(df))


def deposit(df):
    return df.select(F.trim("deposit_id").alias("deposit_id"), F.trim("customer_id").alias("customer_id"),
                     F.col("deposit_amount").cast("decimal(18,2)").alias("deposit_amount"),
                     F.to_date("maturity_date").alias("maturity_date"), "_batch_id", *_tok(df))


def loan(df):
    return df.select(F.trim("loan_id").alias("loan_id"), F.trim("customer_id").alias("customer_id"),
                     F.col("loan_amount").cast("decimal(18,2)").alias("loan_amount"),
                     F.col("interest_rate").cast("decimal(5,2)").alias("interest_rate"), "_batch_id", *_tok(df))


CLEANERS = {"customer": customer, "account": account, "transaction": transaction,
            "branch": branch, "deposit": deposit, "loan": loan}
