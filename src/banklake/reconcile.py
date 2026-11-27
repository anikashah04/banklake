"""Data quality + source-to-target reconciliation (DuckDB over the Parquet lake).

Stages checked per entity:  RAW -> BRONZE -> SILVER MERGE -> SILVER TRANSFORM -> GOLD
The report names the FIRST stage where a check fails, so a discrepancy can be traced to a layer.
"""
import json
import shutil
import time
import uuid
from pathlib import Path

import duckdb

from . import meta
from .common import LAKE, RAW
from .schemas import ENTITIES, ORDER


# ------------------------------------------------------------ small helpers
def _dir(layer, name):
    return LAKE / layer / name


def _has(layer, name):
    return any(_dir(layer, name).glob("*.parquet"))


def _pq(layer, name):
    return f"read_parquet('{_dir(layer, name)}/*.parquet')"


def _qkeys(name, key):
    """Quarantine keys, or an empty relation if nothing was ever quarantined."""
    if _has("quarantine", name):
        return f"SELECT DISTINCT trim({key}) AS _k FROM {_pq('quarantine', name)} WHERE {key} IS NOT NULL"
    return "SELECT NULL::VARCHAR AS _k WHERE false"


def _raw_rows(entity):
    n = 0
    for f in (RAW / entity).glob("*"):
        if f.suffix == ".csv":
            n += max(0, sum(1 for line in open(f) if line.strip()) - 1)
        elif f.suffix == ".json":
            n += sum(1 for line in open(f) if line.strip())
    return n


class _Out:
    def __init__(self, run_id):
        self.run_id, self.rows = run_id, []

    def add(self, entity, stage, check, src, tgt, ok, detail=""):
        r = dict(entity=entity, stage=stage, check_name=check, source_value=str(src), target_value=str(tgt),
                 status="PASS" if ok else "FAIL", detail=detail if isinstance(detail, str) else json.dumps(detail, default=str))
        self.rows.append(r)
        meta.execute("INSERT INTO reconciliation_results(run_id,entity,stage,check_name,source_value,target_value,status,detail,ts) VALUES(?,?,?,?,?,?,?,?,?)",
                     (self.run_id, entity, stage, check, r["source_value"], r["target_value"], r["status"], r["detail"], meta.now()))

    def dq(self, entity, check, value, ok, detail=""):
        meta.execute("INSERT INTO data_quality_results(run_id,entity,check_name,status,value,detail,ts) VALUES(?,?,?,?,?,?,?)",
                     (self.run_id, entity, check, "PASS" if ok else "FAIL", str(value), detail, meta.now()))


# ------------------------------------------------------------ main entry
STAGES = ["BRONZE", "SILVER_MERGE", "SILVER_TRANSFORM", "GOLD"]
STAGE_ORDER = {s: i for i, s in enumerate(STAGES, 1)}
FKS = {"account": [("customer_id", "customer")], "transaction": [("account_id", "account")],
       "deposit": [("customer_id", "customer")], "loan": [("customer_id", "customer")]}


def run_reconciliation(run_id=None) -> dict:
    """Four stages, each compared with the one before it:
    BRONZE (raw -> Bronze), SILVER_MERGE (Bronze -> merged table), SILVER_TRANSFORM (merged -> Silver),
    GOLD (Silver -> reports)."""
    from . import trace as tr
    run_id = run_id or "recon-" + time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4]
    con, out = duckdb.connect(), _Out(run_id)
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731

    if _lakehouse_built():
        _lakehouse(con, out)
        res = {"run_id": run_id, "results": out.rows, "first_failing_stage": _first_failure(out.rows)}
        return res

    for e in ORDER:
        spec, k = ENTITIES[e], ENTITIES[e]["key"]
        absent = next((st for st, layer in [("BRONZE", "bronze"), ("SILVER_MERGE", "silver_merge"), ("SILVER_TRANSFORM", "silver")]
                       if not _has(layer, e)), None)
        if absent:
            out.add(e, absent, "layer_present", "table", "missing", False, "run the pipeline first")
            continue
        b, mg, s = _pq("bronze", e), _pq("silver_merge", e), _pq("silver", e)
        live = f"(SELECT *, trim({k}) AS _k FROM {mg} WHERE NOT _is_deleted AND {k} IS NOT NULL AND trim({k}) <> '')"

        # ---- 1) RAW -> BRONZE: row counts
        raw = _raw_rows(e)
        b_rows = q(f"SELECT count(*) FROM {b}")[0][0]
        b_bad = q(f"SELECT count(*) FROM {_pq('quarantine', 'bronze_' + e)}")[0][0] if _has("quarantine", "bronze_" + e) else 0
        out.add(e, "BRONZE", "raw_rows_vs_bronze_plus_corrupt", raw, b_rows + b_bad, raw == b_rows + b_bad,
                f"bronze={b_rows}, corrupt_quarantined={b_bad}")
        tracked = meta.query("SELECT COALESCE(SUM(rows_loaded),0) n FROM ingested_files WHERE entity=?", (e,))[0]["n"]
        out.add(e, "BRONZE", "file_tracker_loaded_vs_bronze_rows", tracked, b_rows, tracked == b_rows,
                "rows_loaded recorded per file at ingestion vs rows actually in Bronze")

        # ---- 2) BRONZE -> SILVER MERGE: keys, duplicates, CDC
        only_deletes = f"SELECT trim({k}) FROM {b} WHERE {k} IS NOT NULL GROUP BY 1 HAVING bool_and(upper(trim(coalesce(op_type,'I'))) = 'D')"
        miss = q(f"SELECT DISTINCT trim({k}) FROM {b} WHERE {k} IS NOT NULL AND trim({k}) <> '' EXCEPT SELECT trim({k}) FROM {mg} "
                 f"EXCEPT {_qkeys('silver_merge_' + e, k)} EXCEPT {only_deletes}")
        out.add(e, "SILVER_MERGE", "bronze_keys_missing_in_merge", 0, len(miss), not miss,
                f"sample: {[m[0] for m in miss[:10]]}" if miss else "every Bronze key is merged, quarantined or only ever deleted")
        extra = q(f"SELECT trim({k}) FROM {mg} EXCEPT SELECT trim({k}) FROM {b}")
        out.add(e, "SILVER_MERGE", "merge_keys_not_in_bronze", 0, len(extra), not extra,
                f"sample: {[x[0] for x in extra[:10]]}" if extra else "")
        n_m, n_md = q(f"SELECT count(*), count(DISTINCT {k}) FROM {mg}")[0]
        out.add(e, "SILVER_MERGE", "merge_duplicate_keys", n_m, n_md, n_m == n_md, f"{n_m - n_md} duplicate key rows")
        cdc = q(f"WITH ranked AS (SELECT trim({k}) AS _k, upper(trim(coalesce(op_type,'I'))) AS op, row_number() OVER ("
                f"PARTITION BY trim({k}) ORDER BY try_cast(updated_at AS TIMESTAMP) DESC NULLS LAST, try_cast(batch_id AS INTEGER) DESC NULLS LAST, "
                f"_batch_id DESC, _ingest_ts DESC) rn FROM {b} WHERE {k} IS NOT NULL AND trim({k}) <> '' "
                f"AND upper(trim(coalesce(op_type,'I'))) IN ('I','U','D')) "
                f"SELECT r._k FROM ranked r JOIN (SELECT trim({k}) _k, _is_deleted FROM {mg}) m USING (_k) "
                f"WHERE r.rn = 1 AND (r.op = 'D') <> m._is_deleted")
        out.add(e, "SILVER_MERGE", "delete_flag_vs_latest_cdc_operation", 0, len(cdc), not cdc,
                f"keys whose merged delete flag disagrees with their newest source operation: {[x[0] for x in cdc[:10]]}" if cdc
                else "merged delete flags match the newest source operation of every key")

        # ---- 3) SILVER MERGE -> SILVER TRANSFORM: keys, duplicates, nulls, orphans, values, types
        missing = q(f"SELECT DISTINCT _k FROM {live} EXCEPT SELECT {k} FROM {s} EXCEPT {_qkeys('silver_' + e, k)}")
        out.add(e, "SILVER_TRANSFORM", "merge_keys_missing_in_silver", 0, len(missing), not missing,
                f"sample missing keys: {[m[0] for m in missing[:10]]}" if missing else "all live keys are in Silver or quarantined with a reason")
        extra = q(f"SELECT {k} FROM {s} EXCEPT SELECT _k FROM {live}")
        out.add(e, "SILVER_TRANSFORM", "silver_keys_not_in_merge", 0, len(extra), not extra,
                f"sample: {[x[0] for x in extra[:10]]}" if extra else "")
        gone = q(f"SELECT {k} FROM {s} WHERE {k} IN (SELECT trim({k}) FROM {mg} WHERE _is_deleted)")
        out.add(e, "SILVER_TRANSFORM", "deleted_keys_still_in_silver", 0, len(gone), not gone,
                f"sample: {[x[0] for x in gone[:10]]}" if gone else "source deletes were applied")
        n_s, n_d = q(f"SELECT count(*), count(DISTINCT {k}) FROM {s}")[0]
        out.add(e, "SILVER_TRANSFORM", "duplicate_keys", n_s, n_d, n_s == n_d, f"{n_s - n_d} duplicate key rows")
        out.dq(e, "duplicate_keys", n_s - n_d, n_s == n_d)
        for c in spec["mandatory"]:
            nulls = q(f"SELECT count(*) FROM {s} WHERE {c} IS NULL")[0][0]
            out.dq(e, f"null_{c}", nulls, nulls == 0)
        nulls_total = sum(int(r["value"]) for r in meta.query(
            "SELECT value FROM data_quality_results WHERE run_id=? AND entity=? AND check_name LIKE 'null_%'", (run_id, e)))
        out.add(e, "SILVER_TRANSFORM", "null_mandatory_fields", 0, nulls_total, nulls_total == 0, f"checked {spec['mandatory']}")
        for fk, parent in FKS.get(e, []):
            if _has("silver", parent):
                pk = ENTITIES[parent]["key"]
                orphans = q(f"SELECT {k} FROM {s} WHERE {fk} NOT IN (SELECT {pk} FROM {_pq('silver', parent)})")
                out.add(e, "SILVER_TRANSFORM", f"orphan_{fk}_in_silver", 0, len(orphans), not orphans,
                        f"{e}.{fk} -> {parent}.{pk}; sample: {[o[0] for o in orphans[:10]]}" if orphans else f"every {e}.{fk} matches a {parent}")
        if spec["amount"]:
            a = spec["amount"]
            src_sum, tgt_sum = q(f"SELECT (SELECT sum(try_cast({a} AS DECIMAL(18,2))) FROM {live} WHERE _k IN (SELECT {k} FROM {s})), "
                                 f"(SELECT sum({a}) FROM {s})")[0]
            out.add(e, "SILVER_TRANSFORM", f"sum_{a}", src_sum, tgt_sum, src_sum == tgt_sum, "SUM in Silver Merge (kept keys) vs Silver")
            mism = q(f"SELECT s.{k} FROM {s} s JOIN {live} m ON s.{k} = m._k WHERE s.{a} <> try_cast(m.{a} AS DECIMAL(18,2))")
            out.add(e, "SILVER_TRANSFORM", f"row_level_{a}_mismatch", 0, len(mism), not mism,
                    f"sample keys: {[m[0] for m in mism[:10]]}" if mism else "")
        got = {r[0]: r[1] for r in q(f"DESCRIBE SELECT * FROM {s}")}
        bad_t = {c: got.get(c) for c, t in spec["types"].items() if not str(got.get(c, "")).startswith(t)}
        out.add(e, "SILVER_TRANSFORM", "schema_types", str(spec["types"]), str(bad_t or "ok"), not bad_t)
        if _has("quarantine", "silver_" + e):
            br = q(f"SELECT reject_reason, count(*) FROM {_pq('quarantine', 'silver_' + e)} GROUP BY 1 ORDER BY 2 DESC")
            out.add(e, "SILVER_TRANSFORM", "rejection_breakdown", "-", sum(x[1] for x in br), True, {r[0]: r[1] for r in br})

        # ---- trace: every source key missing from Silver must have a recorded reason
        t = tr.trace(e, limit=10)
        out.add(e, "SILVER_TRANSFORM", "unexplained_missing_records", 0, t["unexplained_total"], t["unexplained_total"] == 0,
                json.dumps({"missing_from_silver": t["missing_total"], "explained": t["explained_total"], "unexplained_sample": t["unexplained"][:5]}, default=str))

    from . import vault
    from .masking import masking_rules
    for ent in ("customer", "account"):
        for col in masking_rules(ent):
            try:
                scan = vault.leak_scan(ent, col)
            except Exception as exc:  # noqa: BLE001
                out.add(ent, "SILVER_TRANSFORM", f"pii_leak_scan_{col}", 0, "error", False, str(exc))
                continue
            leaks = {k: v for k, v in scan.items() if not k.startswith("_") and v}
            out.add(ent, "SILVER_TRANSFORM", f"pii_leak_scan_{col}", 0, sum(leaks.values()), not leaks,
                    f"{scan['_checked_values']} clear raw values searched in {[k for k in scan if not k.startswith('_')]}; leaks: {leaks or 'none'}")

    _joins_and_gold(con, out)
    res = {"run_id": run_id, "results": out.rows}
    res["first_failing_stage"] = _first_failure(out.rows)
    return res


# ------------------------------------------------------------ the Silver Merge / Silver Transform / Gold tables the console builds
LH = ("silver_merge_tables", "silver_transform_tables", "gold_tables")


def source_rows(entity) -> int:
    """Records the source held for what Bronze contains today. A repeated load of one period replaces its file and a snapshot replaces
    everything before it, so only the batches still present in Bronze count, each with the figure from its latest successful load."""
    if not _has("bronze", entity):
        return 0
    batches = {r[0] for r in duckdb.connect().execute(f"SELECT DISTINCT _batch_id FROM {_pq('bronze', entity)}").fetchall()}
    latest = {}
    for r in meta.query("SELECT batch_id, records_read FROM ingestion_audit_log WHERE entity=? AND status='SUCCESS' ORDER BY id", (entity,)):
        latest[r["batch_id"]] = r["records_read"] or 0
    return sum(v for k, v in latest.items() if k in batches)


def _latest_date(table, layer):
    """Report date of the most recent successful load of one table."""
    r = meta.query("SELECT report_date FROM layer_audit_log WHERE layer=? AND target_table_name=? AND status='SUCCESS' ORDER BY id DESC LIMIT 1", (layer, table))
    return r[0]["report_date"] if r else None


def _lakehouse_built():
    return _has(LH[0], "customer_merge") or _has(LH[1], "customer_clean")


def _lakehouse(con, out):
    """Checks for the procedure-driven layers. Each stage is compared with the one before it for the latest report date:
    source records -> Bronze, Bronze keys -> merged rows, merged rows -> clean + rejects, and every Gold table against its audit row."""
    from .transform import catalog
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
    one = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
    ents = [p["sources"][0] for p in catalog.PROCS if p["layer"] == "silver_merge"]
    for e in ents:
        if not _has("bronze", e):
            out.add(e, "BRONZE", "layer_present", "table", "missing", False, "ingest this table first")
            continue
        b = _pq("bronze", e)
        b_rows = one(f"SELECT count(*) FROM {b}")
        b_bad = one(f"SELECT count(*) FROM {_pq('quarantine', 'bronze_' + e)}") if _has("quarantine", "bronze_" + e) else 0
        read = source_rows(e)
        out.add(e, "BRONZE", "source_records_vs_bronze", read, b_rows, read == b_rows,
                f"source count is taken from the loads still present in Bronze; {b_bad:,} rows sit in the Bronze quarantine folder" if b_bad else
                "source count is taken from the loads still present in Bronze")
        mg_t, cl_t, rj_t = f"{e}_merge", f"{e}_clean", f"{e}_rejects"
        rd = _latest_date(cl_t, "silver_transform") or _latest_date(mg_t, "silver_merge")   # each table is checked for the report date it was last built for
        if rd is None or not (_has(LH[0], mg_t) and _has(LH[1], cl_t)):
            out.add(e, "SILVER_MERGE", "layer_present", "table", "missing", False, "run Transformation for this table")
            continue
        mg, cl = _pq(LH[0], mg_t), _pq(LH[1], cl_t)
        k = q(f"DESCRIBE SELECT * FROM {mg}")[0][0]
        at = f"report_date = DATE '{rd}'"
        bcols = {r[0] for r in q(f"DESCRIBE SELECT * FROM {b}")}
        op = "AND upper(trim(coalesce(op_type,'I'))) IN ('I','U','D') " if "op_type" in bcols else ""
        upd = (f"AND (try_cast(substr(updated_at,1,10) AS DATE) IS NULL OR try_cast(substr(updated_at,1,10) AS DATE) <= DATE '{rd}')"
               if "updated_at" in bcols else "")
        n_keys = one(f"SELECT count(DISTINCT {k}) FROM {b} WHERE {k} IS NOT NULL AND trim({k}) <> '' {op}{upd}")
        n_m, n_md, n_live = q(f"SELECT count(*), count(DISTINCT {k}), count(*) FILTER (WHERE is_deleted = 0 AND {k} IS NOT NULL AND trim({k}) <> '') FROM {mg} WHERE {at}")[0]
        out.add(e, "SILVER_MERGE", "bronze_keys_vs_merged_rows", n_keys, n_m, n_keys == n_m, f"one merged row per Bronze key as of {rd}")
        out.add(e, "SILVER_MERGE", "merge_duplicate_keys", n_m, n_md, n_m == n_md, f"{n_m - n_md} duplicate key rows")
        n_c = one(f"SELECT count(*) FROM {cl} WHERE {at}")
        n_r = one(f"SELECT count(*) FROM {_pq(LH[1], rj_t)} WHERE {at}") if _has(LH[1], rj_t) else 0
        out.add(e, "SILVER_TRANSFORM", "live_merged_vs_clean_plus_rejects", n_live, n_c + n_r, n_live == n_c + n_r,
                f"clean={n_c:,}, rejected with a reason={n_r:,}")
        d_c = one(f"SELECT count(*) - count(DISTINCT {k}) FROM {cl} WHERE {at}")
        out.add(e, "SILVER_TRANSFORM", "duplicate_keys", n_c, n_c - d_c, d_c == 0, f"{d_c} duplicate key rows")
        gone = one(f"SELECT count(*) FROM {cl} WHERE {at} AND {k} IN (SELECT {k} FROM {mg} WHERE {at} AND is_deleted = 1)")
        out.add(e, "SILVER_TRANSFORM", "deleted_keys_still_in_clean", 0, gone, gone == 0, "source deletes were applied")
    for g in catalog.procs_for("gold"):
        t = g["target"]
        rd = _latest_date(t, "gold")
        if rd is None or not _has(LH[2], t):
            out.add(t, "GOLD", "layer_present", "table", "missing", False, "run Transformation for this table")
            continue
        n = one(f"SELECT count(*) FROM {_pq(LH[2], t)} WHERE report_date = DATE '{rd}'")
        a = meta.query("SELECT output_count FROM layer_audit_log WHERE layer='gold' AND target_table_name=? AND report_date=? AND status='SUCCESS' ORDER BY id DESC LIMIT 1", (t, rd))
        out.add(t, "GOLD", "rows_vs_audit_log", a[0]["output_count"] if a else "none", n, bool(a) and a[0]["output_count"] == n,
                "rows in the table for the report date vs the count the load recorded")


def _joins_and_gold(con, out):
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
    if not all(_has("silver", e) for e in ("customer", "account", "transaction")):
        return
    c, a, t = _pq("silver", "customer"), _pq("silver", "account"), _pq("silver", "transaction")
    # Join-level counts. Unmatched PARENTS (a customer with no loan) are information, not errors.
    n_c = q(f"SELECT count(*) FROM {c}")[0][0]
    n_ca = q(f"SELECT count(*) FROM {c} WHERE customer_id IN (SELECT customer_id FROM {a})")[0][0]
    n_cat = q(f"SELECT count(*) FROM {c} WHERE customer_id IN (SELECT a.customer_id FROM {a} a JOIN {t} t ON a.account_id=t.account_id)")[0][0]
    no_acc = [r[0] for r in q(f"SELECT customer_id FROM {c} WHERE customer_id NOT IN (SELECT customer_id FROM {a}) LIMIT 5")]
    funnel = [dict(stage="silver customers", n=n_c),
              dict(stage="with >=1 account", n=n_ca, lost=n_c - n_ca, sample_unmatched=no_acc),
              dict(stage="with >=1 transaction", n=n_cat, lost=n_ca - n_cat)]
    out.add("customer", "SILVER_TRANSFORM", "join_funnel", n_c, n_cat, True,
            {"funnel": funnel, "note": "Unmatched rows are not necessarily errors - e.g. a customer may have no transactions."})
    for child, fk, label in [("account", "customer_id", "accounts"), ("deposit", "customer_id", "deposits"), ("loan", "customer_id", "loans")]:
        if _has("silver", child):
            n = q(f"SELECT count(*) FROM {c} WHERE customer_id NOT IN (SELECT {fk} FROM {_pq('silver', child)})")[0][0]
            out.add("customer", "SILVER_TRANSFORM", f"info_customers_without_{label}", n_c, n, True, "valid: a customer need not have this product")
    # 4) SILVER TRANSFORM -> GOLD
    if _has("gold", "customer_360"):
        g = _pq("gold", "customer_360")
        n_g, n_gd = q(f"SELECT count(*), count(DISTINCT customer_id) FROM {g}")[0]
        out.add("customer", "GOLD", "gold_customer_360_rows_vs_silver", n_c, n_g, n_c == n_g, "left joins must not drop customers")
        out.add("customer", "GOLD", "gold_customer_360_duplicate_keys", n_g, n_gd, n_g == n_gd, f"{n_g - n_gd} duplicate customers")
        nulls = q(f"SELECT count(*) FROM {g} WHERE customer_id IS NULL")[0][0]
        out.add("customer", "GOLD", "gold_customer_360_null_keys", 0, nulls, nulls == 0)
        src = q(f"SELECT sum(balance) FROM {a}")[0][0]
        tgt = q(f"SELECT sum(total_balance) FROM {g}")[0][0]
        out.add("account", "GOLD", "gold_customer_balance_vs_silver", src, tgt, src == tgt, "every Silver account belongs to a Silver customer")
    if _has("gold", "branch_performance") and _has("silver", "branch"):
        src = q(f"SELECT sum(balance) FROM {a} WHERE branch_id IN (SELECT branch_id FROM {_pq('silver', 'branch')})")[0][0]
        tgt = q(f"SELECT sum(total_balance) FROM {_pq('gold', 'branch_performance')}")[0][0]
        out.add("account", "GOLD", "gold_branch_balance_vs_silver", src, tgt, src == tgt)
    if _has("gold", "transaction_summary"):
        src = q(f"SELECT sum(amount) FROM {t}")[0][0]
        tgt = q(f"SELECT sum(total_amount) FROM {_pq('gold', 'transaction_summary')}")[0][0]
        out.add("transaction", "GOLD", "gold_txn_amount_vs_silver", src, tgt, src == tgt)
        n_src = q(f"SELECT count(*) FROM {t}")[0][0]
        n_tgt = q(f"SELECT sum(n_transactions) FROM {_pq('gold', 'transaction_summary')}")[0][0]
        out.add("transaction", "GOLD", "gold_txn_count_vs_silver", n_src, n_tgt, n_src == n_tgt)
    for ent, gold, gcol, scol in [("loan", "loan_summary", "total_loan_amount", "loan_amount"), ("deposit", "deposit_summary", "total_deposits", "deposit_amount")]:
        if _has("gold", gold) and _has("silver", ent):
            src = q(f"SELECT sum({scol}) FROM {_pq('silver', ent)}")[0][0]
            tgt = q(f"SELECT sum({gcol}) FROM {_pq('gold', gold)}")[0][0]
            out.add(ent, "GOLD", f"gold_{ent}_amount_vs_silver", src, tgt, src == tgt, "joins to customer must not drop rows")


def _first_failure(rows):
    fails = [r for r in rows if r["status"] == "FAIL"]
    if not fails:
        return None
    r = min(fails, key=lambda r: STAGE_ORDER[r["stage"]])
    return {"stage": r["stage"], "entity": r["entity"], "check": r["check_name"], "detail": r["detail"]}


def format_report(res: dict) -> str:
    lines = [f"Reconciliation run {res['run_id']}", "-" * 78]
    for r in res["results"]:
        if r["check_name"] in ("rejection_breakdown", "join_funnel") or r["check_name"].startswith("info_"):
            lines.append(f"  info  {r['entity']:11} {r['check_name']:34} {r['detail'][:60]}")
        else:
            lines.append(f"  {r['status']:5} {r['entity']:11} {r['check_name']:34} src={r['source_value'][:14]:14} tgt={r['target_value'][:14]}")
    fails = sum(r["status"] == "FAIL" for r in res["results"])
    lines.append("-" * 78)
    lines.append(f"{len(res['results'])} checks, {fails} failed")
    if res["first_failing_stage"]:
        f = res["first_failing_stage"]
        lines.append(f"First failing stage: {f['stage']} / {f['entity']} / {f['check']}  ({f['detail'][:100]})")
    return "\n".join(lines)


# ------------------------------------------------------------ demo: break Silver on purpose
def inject_discrepancy(entity="account", mode="delete", n=25) -> dict:
    """Corrupt the Silver table so the reconciliation engine has something to find."""
    spec = ENTITIES[entity]
    k = spec["key"]
    path = _dir("silver", entity)
    if not _has("silver", entity):
        raise FileNotFoundError("Silver table does not exist yet - run the pipeline first")
    con = duckdb.connect()
    src = f"read_parquet('{path}/*.parquet')"
    ids = [r[0] for r in con.execute(f"SELECT {k} FROM {src} ORDER BY {k} LIMIT {n}").fetchall()]
    in_list = ",".join(f"'{i}'" for i in ids)
    if mode == "delete":
        sel = f"SELECT * FROM {src} WHERE {k} NOT IN ({in_list})"
    elif mode == "duplicate":
        sel = f"SELECT * FROM {src} UNION ALL SELECT * FROM {src} WHERE {k} IN ({in_list})"
    elif mode == "alter":
        a = spec["amount"]
        if not a:
            raise ValueError(f"{entity} has no amount column to alter")
        cols = [f"CASE WHEN {k} IN ({in_list}) THEN CAST({c} + 1 AS DECIMAL(18,2)) ELSE {c} END AS {c}" if c == a else c
                for c in [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]]
        sel = f"SELECT {', '.join(cols)} FROM {src}"
    else:
        raise ValueError(mode)
    tmp = Path(str(path) + "__inj")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    con.execute(f"COPY ({sel}) TO '{tmp}/part-injected.parquet' (FORMAT PARQUET)")
    shutil.rmtree(path)
    tmp.rename(path)
    meta.log_security("demo", "INJECT_DISCREPANCY", f"silver/{entity}", f"mode={mode} n={len(ids)}")
    return {"entity": entity, "mode": mode, "affected_keys": ids[:10], "n": len(ids)}
