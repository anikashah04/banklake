"""Configurable PII masking (Spark + a pure-Python twin used for previews/tests)."""
import re

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .common import MASK_KEY
from .meta import ROLES, execute, log_security, query

RULES = ("NAME", "EMAIL", "ACCOUNT", "PHONE", "FULL", "REDACT")


# ---------- pure Python (dashboard preview, unit tests) ----------
def mask_value(rule: str, v):
    if v is None:
        return None
    v = str(v)
    if rule == "NAME":
        return re.sub(r"(?<=\w)\w", "*", v)
    if rule == "EMAIL":
        return re.sub(r"^(.)[^@]*(@.*)$", r"\1****\2", v)
    if rule in ("ACCOUNT", "PHONE"):
        return "******" + v[-4:]
    if rule == "FULL":
        return "*" * len(v)
    if rule == "REDACT":
        return "[REDACTED]"
    raise ValueError(f"unknown masking rule {rule}")


# ---------- Spark ----------
def mask_col(rule: str, c: Column) -> Column:
    if rule == "NAME":
        return F.regexp_replace(c, r"(?<=\w)\w", "*")
    if rule == "EMAIL":
        return F.regexp_replace(c, r"^(.)[^@]*(@.*)$", "$1****$2")
    if rule in ("ACCOUNT", "PHONE"):
        return F.concat(F.lit("******"), F.substring(c, -4, 4))
    if rule == "FULL":
        return F.regexp_replace(c, r"(?s).", "*")
    if rule == "REDACT":
        return F.when(c.isNull(), F.lit(None)).otherwise(F.lit("[REDACTED]"))
    raise ValueError(f"unknown masking rule {rule}")


def masking_rules(entity: str) -> dict:
    rows = query("SELECT column_name, rule FROM masking_metadata WHERE entity=? AND is_active='Y'", (entity,))
    return {r["column_name"]: r["rule"] for r in rows}


def apply_masking(df: DataFrame, entity: str, actor: str = "pipeline") -> DataFrame:
    """Mask every configured column present in df. Logs the operation."""
    applied = []
    for col, rule in masking_rules(entity).items():
        if col in df.columns:
            df = df.withColumn(col, mask_col(rule, F.col(col)))
            applied.append(f"{col}:{rule}")
    if applied:
        log_security(actor, "MASK", entity, ", ".join(applied))
    return df


def tokenize_expr(c: Column) -> Column:
    """Salted SHA-256 pseudonym (joinable, not reversible without the key). Python twin: vault.token_for."""
    return F.substring(F.sha2(F.concat(c, F.lit(MASK_KEY)), 256), 1, 16)


def tokenize(df: DataFrame, col: str, new_col: str) -> DataFrame:
    return df.withColumn(new_col, tokenize_expr(F.col(col)))


# ---------- configuration (every change is audited) ----------
def set_rule(entity: str, column: str, rule: str, active: bool = True, actor: str = "admin") -> None:
    """Add or change the masking rule for a column. Takes effect the next time Silver Transform runs."""
    if rule not in RULES:
        raise ValueError(f"unknown masking rule {rule!r}; choose from {RULES}")
    execute("INSERT OR REPLACE INTO masking_metadata(entity, column_name, rule, is_active) VALUES(?,?,?,?)",
            (entity, column, rule, "Y" if active else "N"))
    log_security(actor, "MASKING_RULE_SET", f"{entity}.{column}", f"rule={rule} active={'Y' if active else 'N'}")


def grant_unmask(role: str, entity: str, column: str, actor: str = "admin") -> None:
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r}")
    execute("INSERT OR IGNORE INTO role_permissions(role, entity, column_name) VALUES(?,?,?)", (role, entity, column))
    log_security(actor, "UNMASK_GRANTED", f"{entity}.{column}", f"role={role}")


def revoke_unmask(role: str, entity: str, column: str, actor: str = "admin") -> None:
    execute("DELETE FROM role_permissions WHERE role=? AND entity=? AND column_name=?", (role, entity, column))
    log_security(actor, "UNMASK_REVOKED", f"{entity}.{column}", f"role={role}")
