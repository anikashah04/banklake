"""PII vault: tokenize + mask at Bronze ingestion, keep real values encrypted, unmask only at Gold.

Flow
  source files (clear, restricted raw area)
  -> BRONZE: PII column becomes a masked value, a {col}_token column is added, and
     (token -> encrypted real value) is stored in a SEPARATE vault database.
  -> SILVER MERGE and SILVER TRANSFORM carry the masked values and tokens (Silver Transform's protect() is idempotent).
  -> GOLD customer_360: the pipeline decrypts the vault values back in (audited as UNMASK_AT_GOLD), so Gold and
     reports hold clear PII (synthetic here). Other Gold tables hold no PII. Every vault read is in security_audit_log.

Encryption: AES-256-GCM (authenticated). Key = HKDF-SHA256 over BANKLAKE_MASK_KEY (a demo key here). Every value
gets a fresh random 96-bit nonce, and the ciphertext is bound (as GCM associated data) to its entity|column|token, so a
ciphertext copied to another row, or edited, fails authentication instead of decrypting to something wrong.
Stored blob = 0x02 | nonce | ciphertext+tag. Rows written by the earlier Fernet version (no 0x02 prefix) can still be
read and are converted by migrate_legacy(). In production the key would live in a secrets manager / KMS, not an env var.
Not covered: key rotation (tokens are also derived from the key, so rotating it means re-running Silver Transform).
"""
import base64
import hashlib
import os
import sqlite3

import pandas as pd
from cryptography.fernet import Fernet, InvalidToken
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from pyspark.sql import functions as F

from . import meta
from .common import DATA, MASK_KEY
from .masking import mask_col, masking_rules, tokenize_expr

VAULT_DB = DATA / "pii_vault.db"


GCM_VERSION = b"\x02"


def _key(secret: str | None = None) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b"banklake-vault-v2").derive((secret or MASK_KEY).encode())


def _aad(entity: str, column: str, token: str) -> bytes:
    return f"{entity}|{column}|{token}".encode()


def encrypt(entity: str, column: str, token: str, value: str, secret: str | None = None) -> bytes:
    nonce = os.urandom(12)
    return GCM_VERSION + nonce + AESGCM(_key(secret)).encrypt(nonce, value.encode(), _aad(entity, column, token))


def decrypt(entity: str, column: str, token: str, blob: bytes, secret: str | None = None) -> str:
    """Raises InvalidTag if the key is wrong or the ciphertext/row binding was changed."""
    blob = bytes(blob)
    if blob[:1] == GCM_VERSION:
        return AESGCM(_key(secret)).decrypt(blob[1:13], blob[13:], _aad(entity, column, token)).decode()
    return _legacy_fernet().decrypt(blob).decode()  # rows from the earlier Fernet version


def _legacy_fernet() -> Fernet:
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(("vault:" + MASK_KEY).encode()).digest()))


def migrate_legacy() -> int:
    """Re-encrypt any Fernet-era rows with AES-256-GCM. Returns how many rows were converted."""
    c, n = _conn(), 0
    for ent, col, tok, blob in c.execute("SELECT entity, column_name, token, cipher FROM pii_vault").fetchall():
        if bytes(blob)[:1] == GCM_VERSION:
            continue
        try:
            clear = _legacy_fernet().decrypt(bytes(blob)).decode()
        except InvalidToken:
            continue
        c.execute("UPDATE pii_vault SET cipher=? WHERE entity=? AND column_name=? AND token=?", (encrypt(ent, col, tok, clear), ent, col, tok))
        n += 1
    c.commit()
    c.close()
    if n:
        meta.log_security("vault", "MIGRATE_LEGACY_ENCRYPTION", "pii_vault", f"{n} rows Fernet -> AES-256-GCM")
    return n


def token_for(value) -> str:
    """Python twin of the Spark token expression (must stay identical)."""
    return hashlib.sha256((str(value) + MASK_KEY).encode()).hexdigest()[:16]


def _conn():
    DATA.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(VAULT_DB, timeout=30)
    c.execute("CREATE TABLE IF NOT EXISTS pii_vault(entity TEXT, column_name TEXT, token TEXT, cipher BLOB, "
              "PRIMARY KEY(entity, column_name, token))")
    return c


def store(entity: str, column: str, values) -> int:
    c = _conn()
    rows = []
    for v in values:
        if v is not None and str(v).strip():
            t = token_for(v)
            rows.append((entity, column, t, encrypt(entity, column, t, str(v))))
    c.executemany("INSERT OR IGNORE INTO pii_vault VALUES(?,?,?,?)", rows)
    c.commit()
    c.close()
    return len(rows)


def protect(df, entity: str, actor: str = "silver_transform", action: str = "MASK_AT_TRANSFORM"):
    """Store real values in the vault, then replace them by masked value + token. Idempotent: a column that
    already has its {col}_token (masked at Bronze) is left alone, so a second call never re-masks a mask."""
    rules = {c: r for c, r in masking_rules(entity).items()
             if c in df.columns and f"{c}_token" not in df.columns}
    if not rules:
        return df
    migrate_legacy()
    df = df.cache()
    for col in rules:
        vals = [r[0] for r in df.select(col).where(F.col(col).isNotNull() & (F.trim(F.col(col)) != "")).distinct().collect()]
        store(entity, col, vals)
    for col, rule in rules.items():
        df = df.withColumn(f"{col}_token", tokenize_expr(F.col(col))).withColumn(col, mask_col(rule, F.col(col)))
    meta.log_security(actor, action, entity, ", ".join(f"{c}:{r}" for c, r in rules.items()))
    return df


def protect_pandas(df, entity: str, actor: str = "bronze", action: str = "MASK_AT_BRONZE", audit: bool = True):
    """pandas twin of protect() for the source ingestion engine: same AES-256-GCM vault (store), same token
    (token_for) and same masking rules (mask_value). Idempotent in the same way: a column that already has its
    {col}_token is left alone. Returns the DataFrame with masked columns plus {col}_token columns."""
    from .masking import mask_value
    rules = {c: r for c, r in masking_rules(entity).items()
             if c in df.columns and f"{c}_token" not in df.columns}
    if not rules:
        return df
    df = df.copy()
    for col, rule in rules.items():
        s = df[col]
        has = s.notna()
        store(entity, col, s[has & (s.astype(str).str.strip() != "")].unique())
        df[f"{col}_token"] = s.map(lambda v: token_for(v) if v is not None and v == v else None)
        df[col] = s.map(lambda v, r=rule: mask_value(r, v) if v is not None and v == v else None)
    if audit:
        meta.log_security(actor, action, entity, ", ".join(f"{c}:{r}" for c, r in rules.items()))
    return df


# ------------------------------------------------------------------ authorization
def can_unmask(role: str, entity: str, column: str) -> bool:
    rows = meta.query("SELECT 1 FROM role_permissions WHERE role=? AND (entity='*' OR entity=?) AND (column_name='*' OR column_name=?)",
                      (role, entity, column))
    return bool(rows)


def reveal(role: str, entity: str, column: str, tokens, _system: bool = False, _action: str = "UNMASK") -> dict:
    """token -> clear value. Raises PermissionError if the role may not unmask this column. Always audited.
    _system is used only by the Gold build (unmask_for_gold), which is a pipeline step, not a person."""
    if not _system and not can_unmask(role, entity, column):
        meta.log_security(role, "UNMASK_DENIED", f"{entity}.{column}", "role not permitted")
        raise PermissionError(f"role '{role}' may not unmask {entity}.{column}")
    tokens = sorted({t for t in tokens if t})
    out, c, failed = {}, _conn(), 0
    for i in range(0, len(tokens), 500):
        chunk = tokens[i:i + 500]
        q = f"SELECT token, cipher FROM pii_vault WHERE entity=? AND column_name=? AND token IN ({','.join('?' * len(chunk))})"
        for tok, cipher in c.execute(q, [entity, column, *chunk]):
            try:
                out[tok] = decrypt(entity, column, tok, cipher)
            except (InvalidTag, InvalidToken):
                out[tok] = None
                failed += 1
    c.close()
    if failed:
        meta.log_security(role, "VAULT_AUTH_FAILED", f"{entity}.{column}", f"{failed} values failed authentication (wrong key or tampered)")
    meta.log_security(role, _action, f"{entity}.{column}", f"{len(out) - failed} values revealed")
    return out


def unmask_for_gold(spark, df, entity: str, columns, actor: str = "gold_pipeline"):
    """Gold is the first unmasked layer. For each column: read its tokens, decrypt them from the vault
    (audited as UNMASK_AT_GOLD) and put the clear value back. Needs {col}_token on df."""
    for col in columns:
        tcol = f"{col}_token"
        toks = [r[0] for r in df.select(tcol).where(F.col(tcol).isNotNull()).distinct().collect()]
        clear = reveal(actor, entity, col, toks, _system=True, _action="UNMASK_AT_GOLD")
        schema = f"{tcol} string, __clear string"
        m = spark.createDataFrame([(t, v) for t, v in clear.items()], schema)
        df = df.join(m, tcol, "left").drop(col).withColumnRenamed("__clear", col)
    return df


def reveal_df(df: pd.DataFrame, entity: str, role: str) -> tuple[pd.DataFrame, list, list]:
    """Replace masked columns by clear values where the role is allowed. Returns (df, revealed, still_masked)."""
    df, revealed, masked = df.copy(), [], []
    for col in masking_rules(entity):
        tok = f"{col}_token"
        if col not in df.columns or tok not in df.columns:
            continue
        if can_unmask(role, entity, col):
            m = reveal(role, entity, col, df[tok].dropna().tolist())
            df[col] = [m.get(t, v) if t else v for t, v in zip(df[tok], df[col])]
            revealed.append(col)
        else:
            masked.append(col)
    return df, revealed, masked


def leak_scan(entity: str, column: str, sample: int = 500) -> dict:
    """Do any clear raw values of this column appear in Bronze / Silver Merge / Silver (the masked layers)? Expected: 0.
    Gold is the unmasked layer by design, so it is not scanned."""
    import duckdb
    from .common import LAKE, RAW
    from .schemas import ENTITIES
    if entity not in ENTITIES:   # source-only tables have no raw files to compare with
        return {}
    vals = []
    for f in sorted((RAW / entity).glob("*.csv"))[:1]:
        d = pd.read_csv(f, dtype=str)
        vals = d[column].dropna().drop_duplicates().head(sample).tolist()
    res = {}
    con = duckdb.connect()
    for layer in ("bronze", "silver_merge", "silver"):
        p = LAKE / layer / entity
        if not any(p.glob("*.parquet")):
            continue
        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}/*.parquet')").fetchall()]
        if column not in cols or not vals:
            continue
        n = con.execute(f"SELECT count(*) FROM read_parquet('{p}/*.parquet') WHERE {column} IN ({','.join('?' * len(vals))})", vals).fetchall()[0][0]
        res[layer] = n
    res["_checked_values"] = len(vals)
    return res


def access_summary() -> list[dict]:
    """Who did what: audit-log counts per actor and action (unmask, denied, masking, configuration changes)."""
    return meta.query("SELECT actor, action, count(*) AS events, max(ts) AS last_event FROM security_audit_log "
                      "GROUP BY actor, action ORDER BY events DESC")
