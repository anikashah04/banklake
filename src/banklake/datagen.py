"""Synthetic banking data generator (100% fictional) with deliberately injected anomalies."""
import json
import re
import shutil

import numpy as np
import pandas as pd
from faker import Faker

from .common import RAW
from .schemas import ENTITIES, SOURCE_SYSTEM

SCALES = {
    "small": dict(customers=1000, accounts=1500, transactions=10000, branches=50, deposits=2000, loans=500),
    "full": dict(customers=10000, accounts=15000, transactions=100000, branches=500, deposits=20000, loans=5000),
}
REGIONS = ["North", "South", "East", "West", "Central"]
SEGMENTS = ["RETAIL", "PREMIUM", "SME", "CORPORATE"]
FMT = "%Y-%m-%d %H:%M:%S"


# Multiplier for every injected data-quality problem. 1.0 = default rates, 0 = perfectly clean data.
_DQ = 1.0


def _k(rate, n, floor=2):
    if _DQ <= 0:
        return 0
    return max(floor, round(rate * n * _DQ))


def _dates(rng, n, start, end, fmt="%Y-%m-%d"):
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    secs = int((e - s).total_seconds())
    return (s + pd.to_timedelta(rng.integers(0, secs, n), unit="s")).strftime(fmt)


def _stamp(entity, df, batch):
    """Add the traceability / CDC columns every source file carries (see schemas.CDC_COLUMNS)."""
    df = df.copy()
    ts = "2025-01-01 00:00:00" if batch == 1 else "2026-07-01 00:00:00"
    if "updated_at" not in df.columns:
        df["updated_at"] = ts
    if "created_at" not in df.columns:
        df["created_at"] = df["updated_at"]
    df["source_system"] = SOURCE_SYSTEM[entity]
    df["batch_id"] = batch
    if "op_type" not in df.columns:
        df["op_type"] = "I"
    return df


def _write(entity, df, fname, extra_raw_lines=(), batch=1):
    d = RAW / entity
    d.mkdir(parents=True, exist_ok=True)
    spec = ENTITIES[entity]
    df = _stamp(entity, df, batch)[spec["columns"]]
    path = d / fname
    if spec["fmt"] == "csv":
        df.to_csv(path, index=False)
    else:
        df.to_json(path, orient="records", lines=True)
        if extra_raw_lines:
            with open(path, "a") as f:
                for line in extra_raw_lines:
                    f.write(line + "\n")
    return path


def read_jsonl(path):
    """Read a JSON-lines source file, skipping the deliberately malformed lines."""
    rows = []
    for line in open(path):
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return pd.DataFrame(rows).astype(str).replace({"None": None, "nan": None})


class _Alloc:
    """Hands out disjoint random row positions so anomalies never overlap."""

    def __init__(self, rng, n):
        self.perm, self.i = rng.permutation(n), 0

    def take(self, k):
        out = self.perm[self.i:self.i + k]
        self.i += k
        return out


def generate_full(scale="full", seed=42, dq_rate=1.0):
    """Batch 1: the initial full load. dq_rate scales every injected problem (0 = clean data)."""
    global _DQ
    _DQ = dq_rate
    n = SCALES[scale]
    rng = np.random.default_rng(seed)
    Faker.seed(seed)
    fake = Faker("en_IN")
    if RAW.exists():
        shutil.rmtree(RAW)
    RAW.mkdir(parents=True)
    manifest = {}
    cities = [fake.city() for _ in range(30)]

    # ---- branches ----
    nb = n["branches"]
    branches = pd.DataFrame({
        "branch_id": [f"B{i:04d}" for i in range(1, nb + 1)],
        "branch_name": [f"{cities[i % 30]} {rng.choice(['Main', 'Market Road', 'Station', 'Mall'])} Branch"
                        for i in range(nb)],
        "region": rng.choice(REGIONS, nb),
    })
    _write("branch", branches, "branch_full.csv")

    # ---- customers ----
    nc = n["customers"]
    first = [fake.first_name() for _ in range(nc)]
    last = [fake.last_name() for _ in range(nc)]
    cust = pd.DataFrame({
        "customer_id": [f"C{i:06d}" for i in range(1, nc + 1)],
        "customer_name": [f"{a} {b}" for a, b in zip(first, last)],
        "email": [re.sub(r"[^a-z0-9.]", "", f"{a}.{b}{i}".lower()) + "@example.com"
                  for i, (a, b) in enumerate(zip(first, last), 1)],
        "phone": rng.integers(6_000_000_000, 9_999_999_999, nc).astype(str),
        "city": rng.choice(cities, nc),
        "segment": rng.choice(SEGMENTS, nc, p=[.6, .2, .15, .05]),
        "updated_at": _dates(rng, nc, "2025-01-01", "2025-12-31", FMT),
    })
    al = _Alloc(rng, nc)
    k_null, k_msg, k_dup = _k(.008, nc), _k(.02, nc), _k(.01, nc)
    cust.loc[al.take(k_null), "customer_name"] = None
    msg = al.take(k_msg)
    cust.loc[msg, "segment"] = [" " + s.lower() + " " for s in cust.loc[msg, "segment"]]
    dup = cust.loc[al.take(k_dup)].copy()  # newer version of the same customer
    dup["city"] = rng.choice(cities, len(dup))
    dup["updated_at"] = _dates(rng, len(dup), "2026-01-01", "2026-03-31", FMT)
    cust_out = pd.concat([cust, dup], ignore_index=True)
    _write("customer", cust_out, "customer_full.csv")
    manifest["customer"] = {"NULL_CUSTOMER_NAME": k_null, "duplicate_rows": k_dup, "messy_segment": k_msg}

    # ---- accounts ----
    na = n["accounts"]
    acc = pd.DataFrame({
        "account_id": [f"A{i:07d}" for i in range(1, na + 1)],
        "customer_id": rng.choice(cust["customer_id"], na),
        "branch_id": rng.choice(branches["branch_id"], na),
        "account_type": rng.choice(["SAVINGS", "CURRENT", "SALARY"], na, p=[.6, .25, .15]),
        "account_number": rng.integers(10**11, 10**12 - 1, na).astype(str),
        "balance": np.round(rng.lognormal(10, 1.2, na), 2).astype(str),
        "opened_date": _dates(rng, na, "2018-01-01", "2025-12-31"),
        "updated_at": _dates(rng, na, "2025-01-01", "2025-12-31", FMT),
    })
    valid_account_ids = acc["account_id"].copy()
    al = _Alloc(rng, na)
    k_nid, k_dt, k_bal, k_orph = _k(.005, na), _k(.005, na), _k(.003, na), _k(.01, na)
    acc.loc[al.take(k_nid), "account_id"] = None
    acc.loc[al.take(k_dt), "opened_date"] = "31-02-2024"
    acc.loc[al.take(k_bal), "balance"] = "N/A"
    acc.loc[al.take(k_orph), "customer_id"] = [f"C9{i:05d}" for i in range(k_orph)]
    _write("account", acc, "account_full.csv")
    manifest["account"] = {"NULL_ACCOUNT_ID": k_nid, "INVALID_OPENED_DATE": k_dt,
                           "INVALID_BALANCE": k_bal, "ORPHAN_CUSTOMER_ID": k_orph}

    # ---- transactions (JSON lines) ----
    nt = n["transactions"]
    txn = pd.DataFrame({
        "transaction_id": [f"T{i:09d}" for i in range(1, nt + 1)],
        "account_id": rng.choice(valid_account_ids, nt),
        "transaction_date": _dates(rng, nt, "2025-01-01", "2026-06-30"),
        "amount": np.round(rng.lognormal(6.5, 1.0, nt), 2),
        "txn_type": rng.choice(["CREDIT", "DEBIT"], nt),
    })
    al = _Alloc(rng, nt)
    k_neg, k_dt, k_orph, k_dup = _k(.008, nt), _k(.005, nt), _k(.01, nt), _k(.007, nt)
    idx = al.take(k_neg)
    txn.loc[idx, "amount"] = -txn.loc[idx, "amount"]
    txn.loc[al.take(k_dt), "transaction_date"] = "2025-13-45"
    txn.loc[al.take(k_orph), "account_id"] = [f"A9{i:06d}" for i in range(k_orph)]
    dups = txn.loc[al.take(k_dup)].copy()
    txn_out = pd.concat([txn, dups], ignore_index=True)
    bad_lines = ['{"transaction_id": "T99999990", "account_id": "A00001', '{"transaction_id": ',
                 "{not json at all"] if _DQ > 0 else []
    _write("transaction", txn_out, "transaction_full.json", bad_lines)
    manifest["transaction"] = {"NEGATIVE_AMOUNT": k_neg, "INVALID_TRANSACTION_DATE": k_dt,
                               "ORPHAN_ACCOUNT_ID": k_orph, "duplicate_rows": k_dup,
                               "malformed_json_lines": len(bad_lines)}

    # ---- deposits ----
    nd = n["deposits"]
    dep = pd.DataFrame({
        "deposit_id": [f"D{i:07d}" for i in range(1, nd + 1)],
        "customer_id": rng.choice(cust["customer_id"], nd),
        "deposit_amount": np.round(rng.lognormal(11, .8, nd), 2),
        "maturity_date": _dates(rng, nd, "2026-07-01", "2031-12-31"),
    })
    k = _k(.005, nd)
    if k:
        dep.loc[rng.choice(nd, k, replace=False), "customer_id"] = [f"C9{i:05d}" for i in range(k)]
    _write("deposit", dep, "deposit_full.csv")
    manifest["deposit"] = {"ORPHAN_CUSTOMER_ID": k}

    # ---- loans ----
    nl = n["loans"]
    loan = pd.DataFrame({
        "loan_id": [f"L{i:06d}" for i in range(1, nl + 1)],
        "customer_id": rng.choice(cust["customer_id"], nl),
        "loan_amount": np.round(rng.lognormal(12.5, .7, nl), 2),
        "interest_rate": np.round(rng.uniform(7, 14, nl), 2),
    })
    k = _k(.005, nl)
    if k:
        loan.loc[rng.choice(nl, k, replace=False), "customer_id"] = [f"C9{i:05d}" for i in range(k)]
    _write("loan", loan, "loan_full.csv")
    manifest["loan"] = {"ORPHAN_CUSTOMER_ID": k}

    state = {"customer": nc, "account": na, "transaction": nt, "inc": 0, "seed": seed, "dq_rate": dq_rate}
    (RAW / "_state.json").write_text(json.dumps(state))
    (RAW / "_anomaly_manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def generate_incremental(seed=None, deletes=False):
    """Batch 2, 3, ...: new + changed customers/accounts and new transactions, each row tagged with
    op_type I/U. With deletes=True it also emits D rows (customers with no accounts, a few
    transactions) carrying the last known image of the record, as a CDC feed would."""
    global _DQ
    state = json.loads((RAW / "_state.json").read_text())
    _DQ = state.get("dq_rate", 1.0)
    state["inc"] += 1
    b = state["inc"]  # file number; the source batch_id written into rows is b + 1 (batch 1 = full load)
    rng = np.random.default_rng((seed or state["seed"]) + b)
    Faker.seed((seed or state["seed"]) + b)
    fake = Faker("en_IN")
    nc, na, nt = state["customer"], state["account"], state["transaction"]
    cities = [fake.city() for _ in range(30)]

    # customers: new + updated
    n_new, n_upd = max(5, int(.03 * nc)), max(5, int(.02 * nc))
    first = [fake.first_name() for _ in range(n_new)]
    last = [fake.last_name() for _ in range(n_new)]
    new_c = pd.DataFrame({
        "customer_id": [f"C{i:06d}" for i in range(nc + 1, nc + n_new + 1)],
        "customer_name": [f"{a} {c}" for a, c in zip(first, last)],
        "email": [re.sub(r"[^a-z0-9.]", "", f"{a}.{c}{nc + i}".lower()) + "@example.com"
                  for i, (a, c) in enumerate(zip(first, last), 1)],
        "phone": rng.integers(6_000_000_000, 9_999_999_999, n_new).astype(str),
        "city": rng.choice(cities, n_new),
        "segment": rng.choice(SEGMENTS, n_new),
        "updated_at": _dates(rng, n_new, "2026-07-01", "2026-10-31", FMT),
    })
    new_c["op_type"] = "I"
    if _DQ > 0:
        new_c.loc[0, "customer_name"] = None  # 1 injected anomaly
    full_c = pd.read_csv(RAW / "customer" / "customer_full.csv", dtype=str)
    pool = full_c[full_c["customer_name"].notna()].drop_duplicates("customer_id")
    upd_c = pool.sample(n_upd, random_state=b).copy()
    upd_c["city"] = rng.choice(cities, n_upd)
    upd_c["updated_at"] = _dates(rng, n_upd, "2026-07-01", "2026-10-31", FMT)
    upd_c["op_type"] = "U"
    parts_c = [new_c, upd_c]
    n_del_c = 0
    if deletes:
        has_acc = set(pd.read_csv(RAW / "account" / "account_full.csv", dtype=str)["customer_id"].dropna())
        free = pool[~pool["customer_id"].isin(has_acc) & ~pool["customer_id"].isin(upd_c["customer_id"])]
        del_c = free.head(max(3, int(.01 * nc))).copy()
        del_c["op_type"] = "D"
        del_c["updated_at"] = _dates(rng, len(del_c), "2026-07-01", "2026-10-31", FMT)
        parts_c.append(del_c)
        n_del_c = len(del_c)
    _write("customer", pd.concat(parts_c), f"customer_inc_{b}.csv", batch=b + 1)

    # accounts: new + balance updates
    a_new, a_upd = max(5, int(.03 * na)), max(5, int(.02 * na))
    cust_ids = [f"C{i:06d}" for i in range(2, nc + n_new + 1)]  # skip the null-name new customer
    new_a = pd.DataFrame({
        "account_id": [f"A{i:07d}" for i in range(na + 1, na + a_new + 1)],
        "customer_id": rng.choice(cust_ids, a_new),
        "branch_id": [f"B{int(i):04d}" for i in rng.integers(1, 51, a_new)],
        "account_type": rng.choice(["SAVINGS", "CURRENT", "SALARY"], a_new),
        "account_number": rng.integers(10**11, 10**12 - 1, a_new).astype(str),
        "balance": np.round(rng.lognormal(10, 1.2, a_new), 2).astype(str),
        "opened_date": _dates(rng, a_new, "2026-01-01", "2026-10-31"),
        "updated_at": _dates(rng, a_new, "2026-07-01", "2026-10-31", FMT),
    })
    full_a = pd.read_csv(RAW / "account" / "account_full.csv", dtype=str)
    ok = full_a[full_a["account_id"].notna() & (full_a["balance"] != "N/A")
                & (full_a["opened_date"] != "31-02-2024") & ~full_a["customer_id"].str.startswith("C9")]
    upd_a = ok.sample(a_upd, random_state=b).copy()
    upd_a["balance"] = np.round(rng.lognormal(10, 1.2, a_upd), 2).astype(str)
    upd_a["updated_at"] = _dates(rng, a_upd, "2026-07-01", "2026-10-31", FMT)
    new_a["op_type"], upd_a["op_type"] = "I", "U"
    _write("account", pd.concat([new_a, upd_a]), f"account_inc_{b}.csv", batch=b + 1)

    # transactions: new only (+ 2 negative, 1 orphan, 1 duplicate)
    t_new = max(20, int(.05 * nt))
    acc_ids = [f"A{i:07d}" for i in range(1, na + a_new + 1)]
    new_t = pd.DataFrame({
        "transaction_id": [f"T{i:09d}" for i in range(nt + 1, nt + t_new + 1)],
        "account_id": rng.choice(acc_ids, t_new),
        "transaction_date": _dates(rng, t_new, "2026-07-01", "2026-10-31"),
        "amount": np.round(rng.lognormal(6.5, 1.0, t_new), 2),
        "txn_type": rng.choice(["CREDIT", "DEBIT"], t_new),
    })
    if _DQ > 0:
        new_t.loc[[0, 1], "amount"] = -new_t.loc[[0, 1], "amount"]
        new_t.loc[2, "account_id"] = "A9999999"
        new_t = pd.concat([new_t, new_t.iloc[[3]]], ignore_index=True)
    new_t["op_type"] = "I"
    n_del_t = 0
    if deletes:
        full_t = read_jsonl(RAW / "transaction" / "transaction_full.json")
        full_t = full_t[full_t["transaction_id"].notna() & ~full_t["amount"].astype(str).str.startswith("-")
                        & (full_t["transaction_date"] != "2025-13-45") & ~full_t["account_id"].astype(str).str.startswith("A9")]
        del_t = full_t.drop_duplicates("transaction_id").head(max(5, int(.002 * nt))).copy()
        del_t["op_type"] = "D"
        n_del_t = len(del_t)
        new_t = pd.concat([new_t, del_t], ignore_index=True)
    _write("transaction", new_t, f"transaction_inc_{b}.json", batch=b + 1)

    state.update(customer=nc + n_new, account=na + a_new, transaction=nt + t_new)
    (RAW / "_state.json").write_text(json.dumps(state))
    return {"batch": b + 1, "new_customers": n_new, "updated_customers": n_upd, "deleted_customers": n_del_c,
            "new_accounts": a_new, "updated_accounts": a_upd, "new_transactions": t_new,
            "deleted_transactions": n_del_t}
