"""One source of truth for record counts. Every page of the console reads these functions, so the overview and the stage pages
cannot disagree.

Counts are the current contents of the lake (Parquet), plus the quarantine folders. Each stage's input is the previous stage's
output, and every row that does not reach the output is accounted for:

    Bronze        input = source records read      output = Bronze rows        rejected = unreadable lines (quarantine)
    Silver Merge  input = Bronze rows              output = merged rows        rejected = failed key checks (quarantine)
                  superseded = input - output - rejected   (older versions of a record replaced by the newest one)
    Silver Transform  input = merged rows          output = Silver rows        rejected = live merged records that failed a business rule and have no valid newest version in Silver
                  deleted = merged rows flagged as deleted at source (not carried into Silver)
                  unexplained = input - output - rejected - deleted   (expected 0; shown as a warning otherwise)

A value that cannot be read is None and is shown as unavailable, never as 0.
"""
import duckdb

from . import meta
from .common import LAKE
from .lineage import GOLD_LINEAGE
from .schemas import ENTITIES, ORDER


def _files(layer: str, name: str) -> str:
    return f"{LAKE / layer / name}/*.parquet"


def _has(layer: str, name: str) -> bool:
    return any((LAKE / layer / name).glob("*.parquet")) if (LAKE / layer / name).exists() else False


def _count(layer: str, name: str, where: str = "") -> int | None:
    if not _has(layer, name):
        return None
    w = f" WHERE {where}" if where else ""
    return duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{_files(layer, name)}'){w}").fetchone()[0]


def _bad(layer_prefix: str, entity: str) -> int:
    """Records currently set aside. Quarantine keeps history, so a key rejected in two runs is one record, not two.
    Bronze quarantine holds unreadable lines (no key), so those are plain row counts."""
    name = f"{layer_prefix}_{entity}"
    if layer_prefix == "bronze" or not _has("quarantine", name):
        return _count("quarantine", name) or 0
    k = ENTITIES[entity]["key"]
    return duckdb.connect().execute(
        f"SELECT count(DISTINCT {k}) + count(*) FILTER (WHERE {k} IS NULL) FROM read_parquet('{_files('quarantine', name)}')").fetchone()[0]


def source_records() -> dict:
    """Records read from the source files that the current Bronze table is made of: every load since the latest FULL
    load of that dataset (a full load replaces Bronze), or every load so far when the dataset has only incremental loads."""
    out = {e: 0 for e in ORDER}
    for e in ORDER:
        full = meta.query("SELECT max(batch_id) b FROM ingestion_audit_log WHERE entity=? AND load_type='FULL' AND status='SUCCESS'", (e,))[0]["b"]
        r = meta.query("SELECT sum(records_read) n FROM ingestion_audit_log WHERE entity=? AND status!='SKIPPED' AND batch_id>=?", (e, full or 0))
        out[e] = r[0]["n"] or 0
    return out


def _transform_split(entity: str, key: str):
    """(currently rejected, earlier version kept) for Silver Transform, judged against the current merged rows.
    Quarantine keeps history, so a key rejected in an old run and valid or deleted now must not count as rejected.
    A live merged record is 'currently rejected' when it is in quarantine and Silver does not hold its newest version."""
    if not (_has("silver_merge", entity) and _has("silver", entity)):
        return None, None
    q = (f"(SELECT DISTINCT {key} AS k FROM read_parquet('{_files('quarantine', 'silver_' + entity)}'))"
         if _has("quarantine", f"silver_{entity}") else f"(SELECT NULL::VARCHAR AS k WHERE false)")
    sql = (f"SELECT count(*) FILTER (WHERE q.k IS NOT NULL AND (s.{key} IS NULL OR s._batch_id < m._batch_id)), "
           f"count(*) FILTER (WHERE q.k IS NOT NULL AND s.{key} IS NOT NULL AND s._batch_id < m._batch_id) "
           f"FROM (SELECT {key}, _batch_id FROM read_parquet('{_files('silver_merge', entity)}') WHERE NOT _is_deleted) m "
           f"LEFT JOIN read_parquet('{_files('silver', entity)}') s ON s.{key} = m.{key} LEFT JOIN {q} q ON q.k = m.{key}")
    return duckdb.connect().execute(sql).fetchone()


def datasets() -> list[dict]:
    """One row per dataset with the full chain Source -> Bronze -> Silver Merge -> Silver Transform."""
    src = source_records()
    rows = []
    for e in ORDER:
        bronze, merged, silver = _count("bronze", e), _count("silver_merge", e), _count("silver", e)
        b_rej, m_rej = _bad("bronze", e), _bad("silver_merge", e)
        s_rej, kept = _transform_split(e, ENTITIES[e]["key"])
        deleted = _count("silver_merge", e, "_is_deleted") if merged is not None else None
        r = dict(dataset=e, source=src.get(e) or None, bronze=bronze, bronze_rejected=b_rej,
                 merge_in=bronze, merge_out=merged, merge_rejected=m_rej,
                 transform_in=merged, transform_out=silver, transform_rejected=s_rej, transform_deleted=deleted, transform_kept_previous=kept)
        r["merge_superseded"] = None if bronze is None or merged is None else bronze - merged - m_rej
        r["transform_unexplained"] = (None if merged is None or silver is None
                                      else merged - (silver - (kept or 0)) - (s_rej or 0) - (deleted or 0))
        rows.append(r)
    return rows


def _sum(rows, key):
    vals = [r[key] for r in rows if r[key] is not None]
    return sum(vals) if vals else None


def stage_totals() -> dict:
    ds = datasets()
    keys = ["source", "bronze", "bronze_rejected", "merge_in", "merge_out", "merge_rejected", "merge_superseded",
            "transform_in", "transform_out", "transform_rejected", "transform_deleted", "transform_kept_previous", "transform_unexplained"]
    t = {k: _sum(ds, k) for k in keys}
    t["rejected_total"] = (t["bronze_rejected"] or 0) + (t["merge_rejected"] or 0) + (t["transform_rejected"] or 0)
    return t


def gold_tables() -> list[dict]:
    return [dict(table=g, rows=_count("gold", g), grain=GOLD_LINEAGE[g][0]) for g in GOLD_LINEAGE]


def has_run() -> bool:
    return bool(meta.query("SELECT 1 FROM pipeline_execution_log LIMIT 1"))
