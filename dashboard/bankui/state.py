"""Backend state for the shell and the overview: everything is computed from the metadata tables and the lake. Nothing is hardcoded."""
import os

import streamlit as st

from banklake import meta, monitor
from banklake.common import LAKE, RAW
from banklake.schemas import ENTITIES, ORDER

STAGES = [("source", "Source Systems"), ("bronze", "Bronze"), ("silver_merge", "Silver Merge"),
          ("silver", "Silver Transform"), ("gold", "Gold"), ("consumption", "Consumption")]
DATASET_NAME = {"customer": "customer_master", "account": "account_master", "transaction": "transaction_details",
                "branch": "branch_master", "deposit": "deposit_details", "loan": "loan_details"}
LAYERS = ["bronze", "silver_merge", "silver", "gold"]


def environment() -> str:
    return "AWS" if os.environ.get("BANKLAKE_ENV", "").lower() == "aws" else "LOCAL"


def aggregate(statuses) -> str:
    s = [x for x in statuses if x]
    if not s:
        return "NOT RUN"
    for bad in ("FAILED", "RUNNING", "BLOCKED"):
        if bad in s:
            return bad
    return "SUCCESS" if "SUCCESS" in s else "SKIPPED"


@st.cache_data(ttl=4, show_spinner=False)
def latest():
    return monitor.latest_per_pipeline()


@st.cache_data(ttl=4, show_spinner=False)
def stage_summary() -> dict:
    rows = latest()
    out = {}
    for layer in LAYERS:
        r = [x for x in rows if (x.get("layer") or x.get("m_layer")) == layer]
        ts = [x["end_ts"] for x in r if x["end_ts"] and x["status"] == "SUCCESS"]
        out[layer] = dict(status=aggregate([x["status"] for x in r]), pipelines=len(r),
                          ok=sum(x["status"] == "SUCCESS" for x in r),
                          rows_in=sum(x["rows_in"] or 0 for x in r), rows_out=sum(x["rows_out"] or 0 for x in r),
                          rejected=sum(x["rows_rejected"] or 0 for x in r), duration=sum(x["duration_s"] or 0 for x in r),
                          last=max(ts) if ts else None)
    ing = meta.query("SELECT count(DISTINCT source_file) files, count(DISTINCT entity) datasets, max(ts) last, "
                     "sum(CASE WHEN status!='SKIPPED' THEN records_read ELSE 0 END) read FROM ingestion_audit_log")[0]
    raw_ok = any((RAW / e).exists() and any((RAW / e).glob("*")) for e in ORDER)
    out["source"] = dict(status="AVAILABLE" if raw_ok else "NO DATA", files=ing["files"] or 0, datasets=ing["datasets"] or 0,
                         records=ing["read"] or 0, last=ing["last"])
    gold = [p for p in (LAKE / "gold").glob("*") if any(p.glob("*.parquet"))] if (LAKE / "gold").exists() else []
    out["consumption"] = dict(status="AVAILABLE" if gold else "NO DATA", gold_tables=len(gold))
    return out


@st.cache_data(ttl=4, show_spinner=False)
def header_state() -> dict:
    last = meta.query("SELECT run_id FROM pipeline_execution_log ORDER BY execution_id DESC LIMIT 1")
    run_id = last[0]["run_id"] if last else None
    st_run = "NOT RUN"
    if run_id:
        st_run = aggregate([r["status"] for r in meta.query("SELECT status FROM pipeline_execution_log WHERE run_id=?", (run_id,))])
    wm = meta.query("SELECT max(last_batch_id) b FROM pipeline_watermark")[0]["b"]
    ok = meta.query("SELECT max(end_ts) t FROM pipeline_execution_log WHERE status='SUCCESS'")[0]["t"]
    return dict(run_id=run_id, batch=wm, status=st_run, last_success=ok)


@st.cache_data(ttl=4, show_spinner=False)
def headline() -> dict:
    rows = latest()
    ing = meta.query("SELECT sum(records_loaded) n FROM ingestion_audit_log WHERE status='SUCCESS'")[0]["n"]
    sv = [x for x in rows if (x.get("layer") or x.get("m_layer")) == "silver" and x["status"] == "SUCCESS"]
    def rate(table):
        last = meta.query(f"SELECT run_id FROM {table} ORDER BY id DESC LIMIT 1")
        if not last:
            return None, 0
        extra = " AND check_name NOT LIKE 'info_%' AND check_name NOT IN ('rejection_breakdown','join_funnel')"
        r = meta.query(f"SELECT status, count(*) n FROM {table} WHERE run_id=?{extra} GROUP BY status", (last[0]["run_id"],))
        tot = sum(x["n"] for x in r)
        return ((sum(x["n"] for x in r if x["status"] == "PASS") / tot) if tot else None), tot

    dq_rate, dq_checks = rate("data_quality_results")
    rec_rate, rec_checks = rate("reconciliation_results")
    return dict(pipelines=meta.query("SELECT count(*) n FROM pipeline_metadata")[0]["n"],
                ok=sum(x["status"] == "SUCCESS" for x in rows), failed=sum(x["status"] == "FAILED" for x in rows),
                running=sum(x["status"] == "RUNNING" for x in rows), ingested=ing, processed=sum(x["rows_out"] or 0 for x in sv),
                rejected=sum(x["rows_rejected"] or 0 for x in rows if x["status"] == "SUCCESS"), dq_rate=dq_rate, dq_checks=dq_checks, rec_rate=rec_rate, rec_checks=rec_checks)
