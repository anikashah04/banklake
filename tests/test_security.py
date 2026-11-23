"""Module 8: AES-256-GCM vault, configurable masking, permission changes and access audit."""
import sqlite3

import pytest


@pytest.fixture(scope="module", autouse=True)
def _meta():
    from banklake import meta
    meta.init_db()


def _row(entity, column, token):
    from banklake import vault
    c = sqlite3.connect(vault.VAULT_DB)
    r = c.execute("SELECT cipher FROM pii_vault WHERE entity=? AND column_name=? AND token=?", (entity, column, token)).fetchone()
    c.close()
    return bytes(r[0])


def _grant_test_role():
    from banklake import masking
    masking.grant_unmask("admin", "sectest", "secret")


def test_gcm_roundtrip_fresh_nonce_and_format():
    from banklake import vault
    a = vault.encrypt("e", "c", "tok", "Zoë Müller 9876")
    b = vault.encrypt("e", "c", "tok", "Zoë Müller 9876")
    assert a != b and a[:1] == b"\x02"            # random nonce: same value never encrypts the same way twice
    assert vault.decrypt("e", "c", "tok", a) == "Zoë Müller 9876"
    assert b"9876" not in a and "Zoë".encode() not in a


def test_tamper_wrong_key_and_row_binding_are_rejected():
    from cryptography.exceptions import InvalidTag
    from banklake import vault
    blob = vault.encrypt("e", "c", "tok", "secret value")
    flipped = bytearray(blob)
    flipped[-1] ^= 1
    with pytest.raises(InvalidTag):
        vault.decrypt("e", "c", "tok", bytes(flipped))           # edited ciphertext
    with pytest.raises(InvalidTag):
        vault.decrypt("e", "c", "tok", blob, secret="another-key")  # wrong key
    for wrong in [("e2", "c", "tok"), ("e", "c2", "tok"), ("e", "c", "tok2")]:
        with pytest.raises(InvalidTag):
            vault.decrypt(*wrong, blob)                           # ciphertext moved to another row


def test_store_reveal_and_substituted_ciphertext_is_audited():
    from banklake import meta, vault
    vault.store("sectest", "secret", ["alpha", "beta"])
    _grant_test_role()
    ta, tb = vault.token_for("alpha"), vault.token_for("beta")
    assert vault.reveal("admin", "sectest", "secret", [ta, tb]) == {ta: "alpha", tb: "beta"}
    assert _row("sectest", "secret", ta)[:1] == b"\x02"
    # an attacker copies beta's ciphertext onto alpha's token
    c = sqlite3.connect(vault.VAULT_DB)
    c.execute("UPDATE pii_vault SET cipher=? WHERE entity='sectest' AND column_name='secret' AND token=?", (_row("sectest", "secret", tb), ta))
    c.commit()
    c.close()
    got = vault.reveal("admin", "sectest", "secret", [ta, tb])
    assert got[ta] is None and got[tb] == "beta"                  # swapped row fails authentication, never returns beta's value
    assert any(r["action"] == "VAULT_AUTH_FAILED" for r in meta.query("SELECT action FROM security_audit_log"))


def test_legacy_fernet_rows_are_readable_and_migrated():
    from banklake import vault
    t = vault.token_for("old-value")
    c = sqlite3.connect(vault.VAULT_DB)
    c.execute("INSERT OR REPLACE INTO pii_vault VALUES('sectest','legacy',?,?)", (t, vault._legacy_fernet().encrypt(b"old-value")))
    c.commit()
    c.close()
    assert vault.decrypt("sectest", "legacy", t, _row("sectest", "legacy", t)) == "old-value"
    assert vault.migrate_legacy() >= 1
    assert _row("sectest", "legacy", t)[:1] == b"\x02"
    assert vault.decrypt("sectest", "legacy", t, _row("sectest", "legacy", t)) == "old-value"
    assert vault.migrate_legacy() == 0


@pytest.mark.parametrize("rule,value,expected", [("FULL", "Aarav 12", "********"), ("REDACT", "anything", "[REDACTED]")])
def test_new_masking_rules_python_and_spark_agree(rule, value, expected):
    from pyspark.sql import functions as F
    from banklake.common import get_spark
    from banklake.masking import mask_col, mask_value
    assert mask_value(rule, value) == expected and mask_value(rule, None) is None
    sp = get_spark()
    got = sp.createDataFrame([(value,), (None,)], "x string").select(mask_col(rule, F.col("x"))).collect()
    assert got[0][0] == expected and got[1][0] is None


def test_configured_rule_drives_protect_and_change_is_audited():
    from pyspark.sql import functions as F
    from banklake import masking, meta, vault
    from banklake.common import get_spark
    with pytest.raises(ValueError):
        masking.set_rule("sectest", "note", "NOT_A_RULE")
    masking.set_rule("sectest", "note", "REDACT")
    df = get_spark().createDataFrame([("k1", "private text"), ("k2", None)], "id string, note string")
    out = vault.protect(df, "sectest").orderBy("id").collect()
    assert out[0]["note"] == "[REDACTED]" and out[0]["note_token"] == vault.token_for("private text")
    assert out[1]["note"] is None
    _ = F  # (kept for symmetry with other tests)
    masking.grant_unmask("admin", "sectest", "note")
    assert vault.reveal("admin", "sectest", "note", [out[0]["note_token"]])[out[0]["note_token"]] == "private text"
    masking.set_rule("sectest", "note", "REDACT", active=False)   # switched off: protect() leaves the column alone
    assert "note_token" not in vault.protect(df, "sectest").columns
    actions = [r["action"] for r in meta.query("SELECT action FROM security_audit_log WHERE target='sectest.note'")]
    assert actions.count("MASKING_RULE_SET") == 2


def test_grant_revoke_enforced_and_audited():
    from banklake import masking, meta, vault
    assert not vault.can_unmask("analyst", "sectest", "grantable")
    masking.grant_unmask("analyst", "sectest", "grantable", actor="admin")
    assert vault.can_unmask("analyst", "sectest", "grantable")
    masking.revoke_unmask("analyst", "sectest", "grantable", actor="admin")
    assert not vault.can_unmask("analyst", "sectest", "grantable")
    with pytest.raises(ValueError):
        masking.grant_unmask("not_a_role", "sectest", "grantable")
    acts = [r["action"] for r in meta.query("SELECT action FROM security_audit_log WHERE target='sectest.grantable'")]
    assert acts == ["UNMASK_GRANTED", "UNMASK_REVOKED"]


def test_denied_unmask_is_audited_and_summarised():
    from banklake import vault
    for _ in range(2):
        with pytest.raises(PermissionError):
            vault.reveal("executive", "sectest", "secret", ["x"])
    s = {(r["actor"], r["action"]): r["events"] for r in vault.access_summary()}
    assert s[("executive", "UNMASK_DENIED")] >= 2 and s[("admin", "UNMASK")] >= 1
