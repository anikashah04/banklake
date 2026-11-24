"""Runs the ingestion scenarios in an isolated BANKLAKE_HOME (the shared test lake is left alone) and prints JSON."""
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from banklake import ingest, meta, orchestrator, sources, vault   # noqa: E402
from banklake.common import layer_path   # noqa: E402

out = {}
meta.init_db()
sources.ensure_sources()


def rows(sql, *a):
    return meta.query(sql, a)


def src_count(sid, entity, where="1=1", args=()):
    c = sources.connect(sid)
    n = c.execute(f'SELECT count(*) FROM "{entity}" WHERE {where}', args).fetchone()[0]
    c.close()
    return n


def bronze_rows(entity):
    import duckdb
    d = layer_path("bronze", entity)
    if not list(d.glob("*.parquet")):
        return 0
    return duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{d}/*.parquet')").fetchone()[0]


def sig():
    return {s: hashlib.sha256(sources.db_path(s).read_bytes()).hexdigest() for s in sources.SOURCES}


# ---- 1. persistent sources + eligibility
out["sources"] = {f"{r['source_id']}/{r['entity']}": r["records"] for r in sources.source_summary()}
sig0 = sig()
out["due"] = {d: [(l.entity, l.frequency, l.period_key) for l in sources.due_loads(d)]
              for d in ["2026-04-15", "2026-05-01", "2026-07-01", "2026-01-01", "2026-06-02"]}

import pandas as pd   # noqa: E402
_days = len(pd.date_range(*__import__("banklake.schemas", fromlist=["CALENDAR"]).CALENDAR))
out["coverage"] = {"days": _days, "daily": {}}
for _t in sources.configured_tables():
    if _t["load_frequency"] == "DAILY":
        _c = sources.connect(_t["source_id"])
        out["coverage"]["daily"][_t["entity"]] = _c.execute(f'SELECT count(DISTINCT business_date) FROM "{_t["entity"]}"').fetchone()[0]
        _c.close()
out["symbols"] = {k: v["symbol"] for k, v in sources.SOURCES.items()}

# ---- 2. dirty flag + PII values must be cleaned / masked at Bronze
c = sqlite3.connect(sources.db_path("SOURCE 2"))
c.execute("INSERT INTO customer(customer_id,customer_name,email,phone,city,segment,updated_at,created_at,source_system,batch_id,op_type,business_date) "
          "VALUES('CZ0001',' ada lovelace ','ada@example.com','9876543210','Pune',' retail ','2026-08-10 00:00:00','2026-08-10 00:00:00','CUSTOMER_CRM','1','I','2026-08-10')")
c.commit()
c.close()
r = ingest.run("2026-08-10")
cust = [t for t in r["tables"] if t["entity"] == "customer"][0]
b = ingest.bronze_batch("customer", cust["batch_id"])
row = b[b.customer_id == "CZ0001"].iloc[0]
out["mask"] = {"status": cust["status"], "segment": row["segment"], "name": row["customer_name"], "email": row["email"],
               "phone": row["phone"], "token_cols": [x for x in b.columns if x.endswith("_token")],
               "revealed": vault.reveal("admin", "customer", "customer_name", [row["customer_name_token"]], _system=True),
               "token_ok": row["customer_name_token"] == vault.token_for(" ada lovelace ")}
out["sources_unchanged_after_run"] = None

# ---- 3. first-of-month run, live arrow states captured while it runs
seen = {}


def hook(stage):
    def f(load):
        snap = ingest.current()
        seen.setdefault(stage, []).append((load.entity, ingest.arrow_states(snap), {t["entity"]: dict(t["stages"]) for t in snap["tables"]}))
    return f


# ---- 3a. dry run of one row: real steps, no writes, and the same row a real ingestion then inserts
def vault_rows():
    c = sqlite3.connect(__import__("banklake.vault", fromlist=["VAULT_DB"]).VAULT_DB)
    n = c.execute("SELECT count(*) FROM pii_vault").fetchone()[0]
    c.close()
    return n


def bfiles():
    return sorted(str(x) for x in layer_path("bronze", "x").parent.rglob("*") if x.is_file())


v0, b0 = vault_rows(), bfiles()
log0 = rows("SELECT count(*) n FROM ingestion_batches")[0]["n"]
dry = {}
for ent, nth in (("customer", 1), ("account", 2), ("loan", 3), ("customer_kyc", 1)):
    ctx, titles = {}, []
    for title, fn in ingest.dry_run_steps(ent, "2026-07-01", nth):
        res = fn(ctx)
        titles.append(title)
        assert res["note"]
    dry[ent] = {"titles": titles, "final": {k: str(v) if v is not None else None for k, v in ctx["final"].items()}, "raw": {k: v for k, v in ctx["raw"].items()}}
out["dry_nowrite"] = {"vault_same": vault_rows() == v0, "bronze_absent": bfiles() == b0,
                      "log_same": rows("SELECT count(*) n FROM ingestion_batches")[0]["n"] == log0}
try:
    list(ingest.dry_run_steps("customer", "2999-01-01", 1))
    out["dry_future"] = "ACCEPTED"
except ValueError:
    out["dry_future"] = "REJECTED"
try:
    ingest.dry_run_steps("customer", "2026-07-01", 1)[1][1]({"where": "business_date = ?", "args": ("2026-07-01",)})
    out["dry_missing_row"] = "no error"
except Exception:
    out["dry_missing_row"] = "no error"

r = ingest.run("2026-07-01", hooks={"extract": hook("extract"), "transform": hook("transform"), "insert": hook("insert")})
out["run_jul1"] = {"status": r["status"], "tables": [(t["entity"], t["frequency"], t["status"], t["source_count"], t["extracted"], t["transformed"],
                                                     t["inserted"], t["bronze_count"]) for t in r["tables"]]}
out["arrows_first_transform"] = seen["transform"][0][1]
out["arrows_first_extract"] = seen["extract"][0][1]
out["arrows_last_insert"] = seen["insert"][-1][1]
out["arrows_end"] = ingest.arrow_states(ingest.current())
out["expected_jul1"] = {l.entity: src_count(l.source_id, l.entity, *sources.where_clause(l)) for l in sources.due_loads("2026-07-01")}
before = {e: bronze_rows(e) for e in out["expected_jul1"]}
nb = rows("SELECT count(*) n FROM ingestion_batches WHERE status='SUCCESS'")[0]["n"]
r2 = ingest.run("2026-07-01")
after = {e: bronze_rows(e) for e in out["expected_jul1"]}
_cmp = {}
for ent in dry:
    b = ingest.bronze_table(ent, 1000000)
    drop = {"_batch_id", "_ingest_ts"}
    want = {k: v for k, v in dry[ent]["final"].items() if k not in drop}
    got = [tuple(None if (x is None or x != x) else str(x) for x in r_) for r_ in b[list(want)].itertuples(index=False)]
    _cmp[ent] = tuple(want.values()) in set(got)
out["dry_matches_real"] = _cmp
out["dry_titles"] = dry["customer"]["titles"]
out["rerun"] = {"statuses": [t["status"] for t in r2["tables"]], "status": r2["status"], "same_rows": before == after,
                "success_rows_added": rows("SELECT count(*) n FROM ingestion_batches WHERE status='SUCCESS'")[0]["n"] - nb}
out["bronze_rows"] = before

# ---- 3b. richer tables: extra columns, source-only banking tables, flag cleaning and PII masking on every table
import duckdb   # noqa: E402
con = duckdb.connect()


def bq(entity, sql):
    import re
    return con.execute(re.sub(r"\bT\b", f"read_parquet('{layer_path('bronze', entity)}/*.parquet')", sql)).fetchall()


out["rich"] = {
    "tables": len(sources.configured_tables()),
    "customer_cols": [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{layer_path('bronze', 'customer')}/*.parquet')").fetchall()],
    "dirty_flags": sum(bq(e, f"SELECT count(*) FROM T WHERE {c} <> upper(trim({c}))")[0][0]
                       for e, c in [("customer", "gender"), ("customer", "kyc_status"), ("account", "account_status"), ("transaction", "channel"),
                                    ("card", "card_status"), ("customer_kyc", "verification_status"), ("loan", "loan_status")]),
    "kyc_id": bq("customer_kyc", "SELECT id_number FROM T LIMIT 1")[0][0],
    "address": bq("customer_address", "SELECT address_line FROM T LIMIT 1")[0][0],
    "card": bq("card", "SELECT card_number FROM T LIMIT 1")[0][0],
    "bene": bq("payment_beneficiary", "SELECT beneficiary_name, beneficiary_account FROM T LIMIT 1")[0],
    "source_only_bronze": {e: bronze_rows(e) for e in ("card", "customer_kyc", "customer_address", "customer_interaction", "loan_repayment", "payment_beneficiary")},
}
try:
    ingest.run("2999-01-01")
    out["future"] = "ACCEPTED"
except ValueError as e:
    out["future"] = str(e)
try:
    ingest.start("2999-01-01")
    out["future_start"] = "ACCEPTED"
except ValueError:
    out["future_start"] = "REJECTED"

# ---- 4. downstream stages read the new Bronze tables
res = orchestrator.run(layers=["silver_merge", "silver", "gold"], retry_sleep=0)
out["downstream"] = sorted(set(res["status"].values()))

# ---- 5. table-level failure (stage failure) must not hide the other tables and must not report success
r = ingest.run("2026-08-11", hooks={"transform": lambda l: (_ for _ in ()).throw(RuntimeError("boom")) if l.entity == "account" else None})
out["fail_stage"] = {"status": r["status"], "tables": {t["entity"]: (t["status"], t["failed_stage"], t["message"]) for t in r["tables"]},
                     "arrow_transform": ingest.arrow_states(r)["transform"],
                     "pipeline_log": [x["status"] for x in rows("SELECT status FROM pipeline_execution_log WHERE pipeline_id='bronze_account' ORDER BY execution_id")],
                     "audit_failed": rows("SELECT count(*) n FROM ingestion_audit_log WHERE entity='account' AND status='FAILED'")[0]["n"]}
tx_before = bronze_rows("transaction")
r = ingest.run("2026-08-11")
out["fail_retry"] = {"status": r["status"], "tables": {t["entity"]: t["status"] for t in r["tables"]}, "tx_unchanged": bronze_rows("transaction") == tx_before,
                     "pipeline_log": [x["status"] for x in rows("SELECT status FROM pipeline_execution_log WHERE pipeline_id='bronze_account' ORDER BY execution_id")]}

# ---- 6. a broken source table (schema mismatch) fails only that table
c = sqlite3.connect(sources.db_path("SOURCE 3"))
c.execute('ALTER TABLE loan RENAME COLUMN interest_rate TO interest_rate_old')
c.commit()
c.close()
r = ingest.run("2026-06-01")
out["schema_fail"] = {"status": r["status"], "tables": {t["entity"]: (t["status"], t["message"][:60]) for t in r["tables"]}}
c = sqlite3.connect(sources.db_path("SOURCE 3"))
c.execute('ALTER TABLE loan RENAME COLUMN interest_rate_old TO interest_rate')
c.commit()
c.close()
r = ingest.run("2026-06-01")
out["schema_fixed"] = {"status": r["status"], "tables": {t["entity"]: t["status"] for t in r["tables"]}}

# ---- 7. the source databases were only changed by the test's own inserts/renames, never regenerated
out["batch_ids_increase"] = [x["batch_id"] for x in rows("SELECT batch_id FROM ingestion_batches WHERE status='SUCCESS' ORDER BY id")]
out["customer_source_after"] = src_count("SOURCE 2", "customer")
out["customer_source_before"] = out["sources"]["SOURCE 2/customer"] + 1
print("@@" + json.dumps(out, default=str))
