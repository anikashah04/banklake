"""Transformation page: Bronze -> Silver Merge -> Silver Transform -> Gold, always visible as one pipeline.
Each layer runs Teradata SELECT -> Athena SELECT -> EMR PySpark -> target table. Every colour on the pipeline is read from the metadata
database and the lake (converted, script generated, loaded, audit rows); nothing is simulated."""
from datetime import date

import pandas as pd
import streamlit as st

from banklake import meta
from banklake.common import layer_path
from banklake.transform import catalog, engine

from ..components import badge, table
from .etl import ARROW, INK, LINE, MUTE, STATE_WORD, esc

TITLE = catalog.LAYER_TITLE
SUB = {"silver_merge": "newest record per key", "silver_transform": "clean, check, summarise", "gold": "report tables"}
FLOW = ("silver_merge", "silver_transform", "gold")
BAD = ("FAILED", "BLOCKED")


# ------------------------------------------------------------------ real state of one procedure
def _live(run, rd, pid):
    if run and run["report_date"] == rd:
        for i in run["items"]:
            if i["proc_id"] == pid:
                return i
    return None


def _nodes(p, rd, run):
    """Teradata / Athena / EMR / Target state of one procedure for the report date."""
    art = engine.artifacts(p["proc_id"]) or {}
    it = _live(run, rd, p["proc_id"])
    stage = it["stage"] if it and it["status"] == "RUNNING" else ""
    last = meta.query("SELECT status FROM layer_audit_log WHERE layer=? AND target_table_name=? AND report_date=? ORDER BY id DESC LIMIT 1",
                      (p["layer"], p["target"], rd))
    last = last[0]["status"] if last else None
    ath = art.get("athena_status")
    td = "processing" if stage == "Converting to Athena" and not art else "done" if art else "waiting"
    at = ("failed" if ath == "UNSUPPORTED" else "processing" if stage == "Converting to Athena" else "done" if ath == "CONVERTED" else "waiting")
    em = "processing" if stage == "Generating EMR script" else "done" if art.get("script") else "waiting"
    if stage == "Running on Spark":
        tg = "processing"
    elif last in BAD:
        tg = "failed"
    elif engine.loaded(p["target"], p["layer"], rd):
        tg = "done"
    else:
        tg = "waiting"
    return dict(td=td, at=at, em=em, tg=tg)


def _worst(states):
    if "failed" in states:
        return "failed"
    if "processing" in states:
        return "processing"
    return "done" if states and all(s == "done" for s in states) else "waiting"


def _path(pid):
    return catalog.upstream(pid) + [catalog.BY_ID[pid]]


def _layer_states(pid, rd, run):
    out = {}
    for p in _path(pid):
        out.setdefault(p["layer"], []).append(_nodes(p, rd, run))
    return {l: dict(n=len(v), done=sum(x["tg"] == "done" for x in v),
                    **{k: _worst([x[k] for x in v]) for k in ("td", "at", "em", "tg")}) for l, v in out.items()}


# ------------------------------------------------------------------ the pipeline picture
def _svg(pid, rd, run):
    st_ = _layer_states(pid, rd, run)
    path = _path(pid)
    bronze = sorted({s for p in path if p["source_db"] == "bronze" for s in p["sources"]})
    b_ok = all(any(layer_path("bronze", t).glob("*.parquet")) for t in bronze)
    o = ['<svg viewBox="0 0 1240 236" xmlns="http://www.w3.org/2000/svg" font-family="Inter,Segoe UI,Arial,sans-serif" text-rendering="geometricPrecision"><defs>']
    for k, col in ARROW.items():
        o.append(f'<marker id="p_{k}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
                 f'<path d="M0,0 L10,5 L0,10 z" fill="{col}"/></marker>')
    o.append("</defs>")

    def arrow(x1, x2, y, state, w=2.5):
        extra = (' stroke-dasharray="8 5"><animate attributeName="stroke-dashoffset" from="26" to="0" dur="0.7s" repeatCount="indefinite"/></path>'
                 if state == "processing" else (' stroke-dasharray="3 4"></path>' if state == "idle" else "></path>"))
        o.append(f'<path d="M{x1},{y} L{x2},{y}" fill="none" stroke="{ARROW[state]}" stroke-width="{w}" marker-end="url(#p_{state})"{extra}')

    # Bronze
    o.append(f'<rect x="8" y="24" width="108" height="166" rx="10" fill="#FFFFFF" stroke="{ARROW["done"] if b_ok else ARROW["failed"]}" stroke-width="1.8"/>')
    o.append('<text x="22" y="52" font-size="12" font-weight="700" letter-spacing=".08em" fill="#374151">BRONZE</text>')
    o.append(f'<text x="22" y="80" font-size="22" font-weight="800" fill="{INK}">{len(bronze)}</text>')
    o.append(f'<text x="22" y="98" font-size="12" fill="#374151">table{"s" if len(bronze) != 1 else ""} read</text>')
    o.append(f'<text x="22" y="130" font-size="11" fill="{MUTE}">PII masked</text><text x="22" y="145" font-size="11" fill="{MUTE}">here only</text>')
    gx = [134, 506, 878]
    first_on = FLOW[0] in st_ or FLOW[1] in st_
    arrow(116, 134, 112, "done" if b_ok and first_on else "idle")
    labels = [("td", "Teradata", "SELECT"), ("at", "Athena", "SELECT"), ("em", "EMR", "PySpark"), ("tg", "Target", "table")]
    for gi, layer in enumerate(FLOW):
        x0, on = gx[gi], layer in st_
        s = st_.get(layer)
        o.append(f'<g opacity="{1 if on else .55}">')
        fill = "#EEF5F1" if layer == "gold" else "#F4F6F9"
        o.append(f'<rect x="{x0}" y="24" width="352" height="166" rx="12" fill="{fill}" stroke="{INK if on else LINE}" stroke-width="{1.8 if on else 1}"/>')
        o.append(f'<text x="{x0 + 14}" y="48" font-size="12.5" font-weight="800" letter-spacing=".08em" fill="{INK}">{TITLE[layer].upper()}</text>')
        sub = SUB[layer] if not (on and s["n"] > 1) else f"{s['n']} tables in this path"
        o.append(f'<text x="{x0 + 14}" y="65" font-size="11.5" fill="{MUTE}">{esc(sub)}</text>')
        for k, (key, a, b) in enumerate(labels):
            nx = x0 + 6 + k * 90
            state = s[key] if on else "idle"
            col = ARROW[state]
            o.append(f'<rect x="{nx}" y="84" width="70" height="56" rx="8" fill="#FFFFFF" stroke="{col}" stroke-width="2"{"" if on else " stroke-dasharray=\"3 4\""}/>')
            o.append(f'<text x="{nx + 35}" y="108" text-anchor="middle" font-size="12.5" font-weight="700" fill="{INK}">{a}</text>')
            o.append(f'<text x="{nx + 35}" y="125" text-anchor="middle" font-size="11.5" fill="#374151">{b}</text>')
            if on:
                o.append(f'<text x="{nx + 35}" y="158" text-anchor="middle" font-size="11" font-weight="600" fill="{col}">{STATE_WORD[state]}</text>')
                if key == "tg" and s["n"] > 1:
                    o.append(f'<text x="{nx + 35}" y="174" text-anchor="middle" font-size="11" fill="{MUTE}">{s["done"]} of {s["n"]}</text>')
            if k:
                arrow(nx - 20, nx, 112, state)
        o.append("</g>")
        if gi:
            prev = FLOW[gi - 1]
            both = prev in st_ and on
            arrow(x0 - 20, x0, 112, st_[prev]["tg"] if both else "idle", 3)
    x = 134
    for k in ("waiting", "processing", "done", "failed"):
        o.append(f'<line x1="{x}" y1="216" x2="{x + 24}" y2="216" stroke="{ARROW[k]}" stroke-width="3"/>')
        o.append(f'<text x="{x + 32}" y="220" font-size="13" font-weight="500" fill="#374151">{STATE_WORD[k]}</text>')
        x += 124
    o.append("</svg>")
    return "".join(o)


# ------------------------------------------------------------------ selected transformation summary
def _status(p, rd, run):
    it = _live(run, rd, p["proc_id"])
    if it and it["status"] == "RUNNING":
        return "RUNNING", it["stage"]
    if it and it["status"] == "WAITING":
        return "NOT RUN", "Waiting for upstream tables"
    last = meta.query("SELECT status, error_message FROM layer_audit_log WHERE layer=? AND target_table_name=? AND report_date=? ORDER BY id DESC LIMIT 1",
                      (p["layer"], p["target"], rd))
    if last and last[0]["status"] in BAD:
        return last[0]["status"], (last[0]["error_message"] or "")[:160]
    ld = engine.loaded(p["target"], p["layer"], rd)
    if ld:
        n = ld["output_count"]
        return "COMPLETED", f"{n:,} rows loaded" if n else "0 rows loaded. Bronze has no matching records for this date; use Load History"
    return "NOT RUN", "Nothing loaded for this report date yet"


def _summary(pid, rd, run):
    p = catalog.BY_ID[pid]
    status, detail = _status(p, rd, run)
    with st.container(border=True):
        c1, c2 = st.columns([4, 1.3], vertical_alignment="center")
        c1.markdown(f"<div style='font-weight:800;font-size:1.15rem;color:{INK}'>{esc(p['target'])}</div>"
                    f"<div style='color:{MUTE};font-size:.88rem'>{esc(TITLE[p['layer']])} · {esc(p['desc'])}</div>", unsafe_allow_html=True)
        c2.markdown(f"<div style='text-align:right'>{badge(status)}</div><div style='text-align:right;color:{MUTE};font-size:.8rem'>{esc(detail)}</div>",
                    unsafe_allow_html=True)
        a, b = st.columns(2)
        a.markdown(f"<span style='color:{MUTE};font-size:.8rem'>{'FED BY SILVER TABLES' if p['layer'] == 'gold' else 'SOURCE TABLES'}</span><br>"
                   + ", ".join(f"`{p['source_db']}.{s}`" for s in p["sources"]), unsafe_allow_html=True)
        b.markdown(f"<span style='color:{MUTE};font-size:.8rem'>TARGET TABLE</span><br>`{p['layer']}.{p['target']}`", unsafe_allow_html=True)
        if p["layer"] == "gold":
            rep = meta.query("SELECT group_concat(report_name, ', ') AS r FROM gold_report_usage WHERE target_table = ?", (p["target"],))
            if rep and rep[0]["r"]:
                st.markdown(f"<span style='color:{MUTE};font-size:.8rem'>USED IN REPORTS</span><br>{esc(rep[0]['r'])}", unsafe_allow_html=True)


# ------------------------------------------------------------------ details, loaded only when requested
@st.dialog("Transformation details", width="large")
def _details(pid, rd):
    p = catalog.BY_ID[pid]
    art = engine.artifacts(pid) or {}
    st.markdown(f"**{p['target']}**  ·  {TITLE[p['layer']]}  ·  report date {date.fromisoformat(rd):%d %b %Y}")
    t = st.tabs(["Teradata SQL", "Athena SQL", "EMR PySpark Script", "Target Data", "Metadata", "Audit Log"])
    with t[0]:
        st.caption("The original SELECT. Teradata only selects; the report date is the placeholder {report_date}.")
        st.code(p["teradata"] + ";", language="sql", height=420)
    with t[1]:
        if art.get("athena_status") == "CONVERTED":
            st.caption("Converted from the Teradata SELECT. SELECT only; lowercase names, schema dots replaced by underscores.")
            st.code(art["athena_sql"] + ";", language="sql", height=400)
            notes = [n for n in (art.get("athena_notes") or "").splitlines() if n.strip()]
            if notes:
                with st.expander("Conversion steps"):
                    for n in notes:
                        st.write("• " + n)
        elif art.get("athena_status") == "UNSUPPORTED":
            st.error("The converter could not convert this SELECT:\n\n" + (art.get("athena_notes") or ""))
        else:
            st.info("Not converted yet. Run the transformation, or convert it now.")
            if st.button("Convert", key="det_conv"):
                engine.convert_all(pid)
                st.rerun(scope="fragment")
    with t[2]:
        if art.get("script"):
            st.caption("Reads the sources, runs the Spark SQL, deletes the report date from the target, appends the new rows and writes the audit log.")
            st.code(art["script"], language="python", height=520)
        else:
            st.info("No script yet. Run the transformation, or convert it first.")
    with t[3]:
        df = engine.preview(p["layer"], p["target"], rd, 100)
        if df is None or df.empty:
            st.caption("This table has no rows for the report date yet.")
        else:
            table(df, height=380)
    with t[4]:
        rows = meta.query("SELECT * FROM gold_transform_metadata WHERE target_table = ?" if p["layer"] == "gold"
                          else "SELECT * FROM silver_transform_metadata WHERE layer = ? AND target_table = ?",
                          (p["target"],) if p["layer"] == "gold" else (p["layer"], p["target"]))
        if rows:
            table(pd.DataFrame([{"Field": k, "Value": "" if v is None else str(v)} for k, v in rows[0].items()]), height=380)
        else:
            st.caption("No metadata row.")
    with t[5]:
        rows = [r for r in engine.audit_rows(layer=p["layer"], limit=400) if r["target_table_name"] == p["target"]][:50]
        if rows:
            table(pd.DataFrame(rows), height=340)
        else:
            st.caption("No audit rows for this table yet.")


@st.dialog("SQL Migration Tool", width="large")
def _migration():
    from .. import helpers, legacy
    from . import procs
    st.caption("Convert any Teradata query or stored procedure on its own. This does not run anything on the lake.")
    a, b = st.tabs(["Stored procedure", "Single query"])
    with a:
        procs.render()
    with b:
        legacy.render("SQL migration", helpers.current_role())


# ------------------------------------------------------------------ page

def _view(pid, rd):
    run = engine.current()
    with st.container(border=True):
        st.image(_svg(pid, rd, run), width="stretch")
    _summary(pid, rd, run)
    if run and run["report_date"] == rd:
        if engine.running():
            its = run["items"]
            st.caption(f"{sum(i['status'] in ('SUCCESS', 'FAILED', 'BLOCKED') for i in its)} of {len(its)} finished · now: {run['current'] or '-'}")
        else:
            if run.get("skipped"):
                st.caption(f"{len(run['skipped'])} tables were left out because their Bronze data does not exist yet.")
            for i in run["items"]:
                if i["status"] in BAD:
                    st.error(f"{i['target']}: {i['status'].lower()}. {i['error']}")
            if run.get("error"):
                st.error(run["error"])


@st.fragment(run_every=0.5)
def _live_fragment(pid, rd):
    st.session_state["_tr_was_running"] = True
    _view(pid, rd)
    if not engine.running():
        st.rerun()


def page():
    from .. import flow
    meta.init_db()
    if not flow.has_bronze():
        flow.no_data()
        return
    run = engine.current()
    busy = engine.running()
    if busy and run:                       # coming back to the page mid-run: show the run, not the default pick
        fp = catalog.BY_ID[run["focus"]]
        grp = "Gold" if fp["layer"] == "gold" else "Silver"
        st.session_state["tr_group"], st.session_state[f"tr_proc_{grp}"] = grp, run["focus"]
        if run["report_date"] != str(st.session_state.get(flow.KEY)):
            st.session_state[flow.KEY] = date.fromisoformat(run["report_date"])
            st.session_state.pop("tr_date", None)
    c1, c2, c3, c4, c5, c6, c7 = st.columns([1.1, 1.0, 2.4, 0.6, 0.8, 0.9, 1.3], vertical_alignment="bottom")
    with c1:
        bd = flow.pick_date("Report date", "tr_date")
    group = c2.segmented_control("Layer", list(catalog.GROUPS), default="Silver", key="tr_group", disabled=busy) or "Silver"
    procs = [p for l in catalog.GROUPS[group] for p in catalog.procs_for(l)]
    ids = [p["proc_id"] for p in procs]
    lab = {p["proc_id"]: f"{p['target']}   ·   {TITLE[p['layer']]}" for p in procs}
    pid = c3.selectbox(f"Transformation  ({len(procs)})", ids, format_func=lab.get, key=f"tr_proc_{group}", disabled=busy)
    rd = bd.isoformat()
    from banklake import ingest
    stale = flow.ingested_through() is None or bd > flow.ingested_through()
    block = busy or stale or ingest.running()
    run_b = c4.button("Run", type="primary", width="stretch", disabled=block, key="tr_run")
    all_b = c5.button("Run All", width="stretch", disabled=block, key="tr_all", help=f"Load every {group} table for this date")
    det_b = c6.button("View Details", width="stretch", key="tr_details")
    mig_b = c7.button("SQL Migration Tool", width="stretch", key="tr_mig")
    if stale:
        flow.not_ingested(bd)
    elif ingest.running():
        st.info("Ingestion is running. Transformation can start when it finishes.")
    if run_b or all_b:
        p = catalog.BY_ID[pid]
        try:
            if run_b:
                engine.start(p["layer"], rd, [p["target"]])
            else:
                engine.start(catalog.GROUPS[group][-1], rd, None)
        except ValueError as e:
            st.error(str(e))
            return
        st.rerun()
    if det_b:
        _details(pid, rd)
    if mig_b:
        _migration()
    if busy:
        _live_fragment(pid, rd)
        return
    if st.session_state.pop("_tr_was_running", False):
        st.cache_data.clear()
    _view(pid, rd)
    if engine.loaded(catalog.BY_ID[pid]["target"], catalog.BY_ID[pid]["layer"], rd):
        if catalog.BY_ID[pid]["layer"] == "gold":
            flow.go("Next: Reporting", "reporting", "tr_next", primary=True)
