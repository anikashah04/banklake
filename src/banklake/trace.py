"""Discrepancy tracing: find source records that did not reach Silver and explain, layer by layer, why.

For one entity it walks  raw files -> Bronze -> Silver Merge -> Silver Transform  and gives every missing key
the FIRST layer where it disappeared and a reason. A record is *explained* when the pipeline has a recorded
reason (rejected with a reason code, soft-deleted by the source). Anything else is *unexplained* - a real
discrepancy (for example rows removed from Silver by hand).
"""
import json
from collections import Counter

import duckdb
import pandas as pd

from .common import LAKE, RAW
from .schemas import ENTITIES


def _pq(layer, name):
    d = LAKE / layer / name
    return f"read_parquet('{d}/*.parquet')" if any(d.glob("*.parquet")) else None


def _keys(con, layer, entity, where="TRUE"):
    src = _pq(layer, entity)
    k = ENTITIES[entity]["key"]
    if not src:
        return set()
    return {r[0] for r in con.execute(f"SELECT DISTINCT trim({k}) FROM {src} WHERE {k} IS NOT NULL AND trim({k}) <> '' AND {where}").fetchall()}


def _quarantine(con, name, entity):
    src = _pq("quarantine", name)
    k = ENTITIES[entity]["key"]
    if not src:
        return {}
    return {r[0]: r[1] for r in con.execute(
        f"SELECT trim({k}), any_value(reject_reason) FROM {src} WHERE {k} IS NOT NULL AND trim({k}) <> '' GROUP BY 1").fetchall()}


def raw_keys(entity):
    """(distinct non-blank keys, total rows, rows with a blank key) from the raw source files."""
    k, keys, rows, blank = ENTITIES[entity]["key"], set(), 0, 0
    for f in sorted((RAW / entity).glob("*")):
        if f.suffix == ".csv":
            vals = pd.read_csv(f, dtype=str)[k].tolist()
        elif f.suffix == ".json":
            vals = []
            for line in open(f):
                if not line.strip():
                    continue
                try:
                    vals.append(json.loads(line).get(k))
                except ValueError:
                    continue  # a corrupt line has no readable key
        else:
            continue
        for v in vals:
            rows += 1
            if v is None or (isinstance(v, float) and pd.isna(v)) or not str(v).strip():
                blank += 1
            else:
                keys.add(str(v).strip())
    return keys, rows, blank


def trace(entity: str, limit: int = 50) -> dict:
    con = duckdb.connect()
    raw, raw_rows, raw_blank = raw_keys(entity)
    bronze = _keys(con, "bronze", entity)
    merge_all = _keys(con, "silver_merge", entity)
    merge_deleted = _keys(con, "silver_merge", entity, "_is_deleted") if _pq("silver_merge", entity) else set()
    silver = _keys(con, "silver", entity)
    merge_q = _quarantine(con, f"silver_merge_{entity}", entity)
    silver_q = _quarantine(con, f"silver_{entity}", entity)

    missing, reasons = sorted(raw - silver), []
    for key in missing:
        if key not in bronze:
            reasons.append((key, "bronze", "never loaded into Bronze (rejected file or corrupt line)", False))
        elif key not in merge_all:
            if key in merge_q:
                reasons.append((key, "silver_merge", f"rejected in Silver Merge: {merge_q[key]}", True))
            else:
                reasons.append((key, "silver_merge", "in Bronze but not in Silver Merge and not quarantined (only deletes for unknown keys are expected)", False))
        elif key in merge_deleted:
            reasons.append((key, "silver_transform", "soft-deleted by the source system (CDC delete)", True))
        elif key in silver_q:
            reasons.append((key, "silver_transform", f"rejected in Silver Transform: {silver_q[key]}", True))
        else:
            reasons.append((key, "silver_transform", "live in Silver Merge but absent from Silver and not quarantined: lost in Silver Transform or removed afterwards", False))
    explained = [r for r in reasons if r[3]]
    unexplained = [r for r in reasons if not r[3]]
    as_dict = lambda r: dict(key=r[0], first_missing_layer=r[1], reason=r[2], explained=r[3])  # noqa: E731
    return {
        "entity": entity,
        "layer_counts": {"raw_rows": raw_rows, "raw_rows_with_blank_key": raw_blank, "raw_distinct_keys": len(raw),
                         "bronze_distinct_keys": len(bronze), "silver_merge_keys": len(merge_all),
                         "silver_merge_soft_deleted": len(merge_deleted), "silver_transform_keys": len(silver),
                         "silver_transform_quarantined_keys": len(silver_q)},
        "missing_total": len(missing),
        "explained_total": len(explained),
        "unexplained_total": len(unexplained),
        "by_reason": dict(Counter(r[2].split(":")[0] + (": " + r[2].split(": ")[1] if r[2].startswith("rejected") else "") for r in reasons)),
        "unexplained": [as_dict(r) for r in unexplained[:limit]],
        "explained": [as_dict(r) for r in explained[:limit]],
        "extra_in_silver": sorted(silver - raw)[:limit],
    }


def format_trace(t: dict) -> str:
    c = t["layer_counts"]
    lines = [f"Trace for {t['entity']}",
             f"  raw: {c['raw_rows']} rows, {c['raw_distinct_keys']} distinct keys ({c['raw_rows_with_blank_key']} rows with no key)",
             f"  bronze: {c['bronze_distinct_keys']} keys | silver merge: {c['silver_merge_keys']} keys "
             f"({c['silver_merge_soft_deleted']} soft-deleted) | silver: {c['silver_transform_keys']} keys",
             f"  source keys missing from Silver: {t['missing_total']}  "
             f"(explained {t['explained_total']}, UNEXPLAINED {t['unexplained_total']})"]
    for reason, n in sorted(t["by_reason"].items(), key=lambda x: -x[1]):
        lines.append(f"    {n:6}  {reason}")
    for u in t["unexplained"][:10]:
        lines.append(f"  !! {u['key']}: first missing at {u['first_missing_layer']} - {u['reason']}")
    if t["extra_in_silver"]:
        lines.append(f"  !! keys in Silver that are not in the source: {t['extra_in_silver'][:10]}")
    return "\n".join(lines)
