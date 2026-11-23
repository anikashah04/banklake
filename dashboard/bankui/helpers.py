"""Shared helpers for the BankLake dashboard (data access, command runner, masking helpers)."""
import json
import re
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from banklake import guide, meta, monitor, reconcile, trace, vault  # noqa: E402,F401
from banklake.common import LAKE, RAW  # noqa: E402,F401
from banklake.masking import mask_value, masking_rules  # noqa: E402,F401
from banklake.schemas import COLUMN_DOCS, ENTITIES, GOLD_FOR, LAYER_DOCS, ORDER  # noqa: E402,F401
from banklake.sqlmigrate import convert  # noqa: E402,F401

# ---------------------------------------------------------------- helpers
def df(sql, params=()):
    return pd.DataFrame(meta.query(sql, params))


def has(layer, name):
    return any((LAKE / layer / name).glob("*.parquet"))


def count(layer, name):
    if not has(layer, name):
        return 0
    return duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{LAKE / layer / name}/*.parquet')").fetchall()[0][0]


def sample(layer, name, n=200):
    return duckdb.connect().execute(f"SELECT * FROM read_parquet('{LAKE / layer / name}/*.parquet') LIMIT {n}").fetchdf()


def cli_seq(*commands):
    """Run main.py commands in order (stop at the first failure), remember the output, refresh the page."""
    outs, rc = [], 0
    with st.spinner("Working... (the data engine needs about 10 seconds to start; processing can take a minute or two)"):
        for args in commands:
            p = subprocess.run([sys.executable, str(ROOT / "main.py"), *args], capture_output=True, text=True)
            lines = [l for l in (p.stdout + p.stderr).splitlines()
                     if not any(x in l for x in ("JAVA_TOOL", "WARN", "log level", "setLogLevel", "Stage "))]
            outs.append(f"$ main.py {' '.join(args)}\n" + "\n".join(lines))
            if p.returncode:
                rc = p.returncode
                break
    st.cache_data.clear()
    st.session_state["_last_action"] = (rc, " then ".join(" ".join(a) for a in commands), "\n\n".join(outs)[-4000:] or "(no output)")
    st.rerun()


_JOB = {"thread": None, "label": "", "out": [], "rc": None, "reported": True}


def job_start(*commands):
    """Run main.py commands in a background thread so the console can show the stages changing while they run."""
    import threading
    if _JOB["thread"] is not None and _JOB["thread"].is_alive():
        return False
    _JOB.update(label=" then ".join("main.py " + " ".join(a) for a in commands), out=[], rc=None, reported=False)

    def work():
        rc = 0
        for args in commands:
            p = subprocess.run([sys.executable, str(ROOT / "main.py"), *args], capture_output=True, text=True)
            lines = [l for l in (p.stdout + p.stderr).splitlines() if not any(x in l for x in ("JAVA_TOOL", "WARN", "log level", "setLogLevel", "Stage "))]
            _JOB["out"].append(f"$ main.py {' '.join(args)}\n" + "\n".join(lines))
            if p.returncode:
                rc = p.returncode
                break
        _JOB["rc"] = rc
    t = threading.Thread(target=work, daemon=True)
    _JOB["thread"] = t
    t.start()
    return True


def job_running():
    return _JOB["thread"] is not None and _JOB["thread"].is_alive()


def job_finished_unreported():
    """True once, right after a background job ends: the caller then shows the result."""
    if _JOB["thread"] is not None and not _JOB["thread"].is_alive() and not _JOB["reported"]:
        _JOB["reported"] = True
        st.cache_data.clear()
        st.session_state["_last_action"] = (_JOB["rc"], _JOB["label"], "\n\n".join(_JOB["out"])[-4000:] or "(no output)")
        return True
    return False


def job_label():
    return _JOB["label"]


def cli(*args):
    cli_seq(args)


def pq(layer, name):
    return f"read_parquet('{LAKE / layer / name}/*.parquet')"


def Q(sql, params=()):
    """Run a DuckDB query over the Parquet lake and return a DataFrame."""
    return duckdb.connect().execute(sql, list(params)).fetchdf()


@st.cache_data(ttl=20, show_spinner=False)
def raw_df(entity, stamp):
    """All raw source files of an entity as text (malformed JSON lines are kept and flagged)."""
    frames = []
    for f in sorted((RAW / entity).glob("*")):
        if f.suffix == ".csv":
            d = pd.read_csv(f, dtype=str)
        elif f.suffix == ".json":
            rows = []
            for line in open(f):
                if not line.strip():
                    continue
                try:
                    rows.append({k: (None if v is None else str(v)) for k, v in json.loads(line).items()})
                except json.JSONDecodeError:
                    rows.append({"_malformed_line": line.strip()})
            d = pd.DataFrame(rows)
        else:
            continue
        d["_source_file"] = f.name
        frames.append(d)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def raw_stamp(entity):
    return tuple((f.name, f.stat().st_mtime) for f in (RAW / entity).glob("*")) if (RAW / entity).exists() else ()


def last_status_by_pipeline():
    rows = meta.query("SELECT * FROM pipeline_execution_log WHERE execution_id IN (SELECT max(execution_id) FROM pipeline_execution_log GROUP BY pipeline_id)")
    return {r["pipeline_id"]: r for r in rows}


def flow_dot():
    """The real pipeline DAG (from pipeline_metadata + dependencies), coloured by last status."""
    colour = {"SUCCESS": "#c8e6c9", "FAILED": "#ffcdd2", "BLOCKED": "#ffe0b2", "SKIPPED": "#e0e0e0", "RUNNING": "#bbdefb"}
    last = last_status_by_pipeline()
    metas = meta.query("SELECT * FROM pipeline_metadata")
    lines = ["digraph G { rankdir=LR; bgcolor=transparent; node [shape=box, style=\"rounded,filled\", fontname=Helvetica, fontsize=10];"]
    for layer in ["bronze", "silver", "gold"]:
        lines.append(f'subgraph cluster_{layer} {{ label="{layer.upper()}"; fontname=Helvetica; color="#9e9e9e";')
        for m in [m for m in metas if m["target_layer"] == layer]:
            r = last.get(m["pipeline_id"])
            fill = colour.get(r["status"] if r else None, "#ffffff")
            txt = m["pipeline_id"].replace(layer + "_", "")
            if r and r["status"] == "SUCCESS":
                txt += f"\\n{r['rows_in']:,} in -> {r['rows_out']:,} out"
                if r["rows_rejected"]:
                    txt += f"\\n{r['rows_rejected']:,} rejected"
            elif r:
                txt += f"\\n{r['status']}"
            if m["is_active"] != "Y":
                txt += "\\n(inactive)"
            lines.append(f'"{m["pipeline_id"]}" [label="{txt}", fillcolor="{fill}"];')
        lines.append("}")
    lines.append('"raw files" [shape=cylinder, fillcolor="#fff9c4"];')
    for m in [m for m in metas if m["target_layer"] == "bronze"]:
        lines.append(f'"raw files" -> "{m["pipeline_id"]}";')
    for d in meta.query("SELECT * FROM pipeline_dependencies"):
        lines.append(f'"{d["depends_on"]}" -> "{d["pipeline_id"]}";')
    lines.append("}")
    return "\n".join(lines)


def inr(x):
    """Indian-style short money format."""
    x = float(x or 0)
    for div, unit in ((1e7, "Cr"), (1e5, "L"), (1e3, "K")):
        if abs(x) >= div:
            return f"₹{x / div:,.2f} {unit}"
    return f"₹{x:,.0f}"


def current_role() -> str:
    """The simulated role chosen in the sidebar (role simulation, not authentication)."""
    return st.session_state.get("sim_role", "analyst")


def show_pii(frame, entity, note=True):
    """Apply the role's unmask rights to a frame carrying *_token columns; hide the tokens."""
    role = current_role()
    shown, revealed, masked = vault.reveal_df(frame, entity, role)
    if note:
        if revealed:
            st.caption(f"🔓 As **{role}** you see clear values for: {', '.join(revealed)} (audited).")
        if masked:
            st.caption(f"🔒 Still masked for **{role}**: {', '.join(masked)}.")
    return shown.drop(columns=[c for c in shown.columns if c.endswith("_token")])


def mask_clear(df, entity):
    """Bronze / raw / merge-stage data holds clear synthetic PII on purpose; the dashboard never shows it unmasked."""
    out = df.copy()
    for col, rule in masking_rules(entity).items():
        if col in out.columns:
            out[col] = out[col].map(lambda v, r=rule: mask_value(r, v) if pd.notna(v) else v)
    if "_corrupt_record" in out.columns:
        out["_corrupt_record"] = out["_corrupt_record"].map(lambda v: "[hidden: may contain PII]" if pd.notna(v) else v)
    return out


GOLD_TABLES = ["customer_360", "branch_performance", "deposit_summary", "loan_summary", "transaction_summary"]
