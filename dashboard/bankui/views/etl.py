"""The five pages of the console: the ETL pipeline, and one page per part of it. Every number is read from the lake or the logs."""
import html
import math
import time
from datetime import date

import pandas as pd
import streamlit as st

from banklake import ingest, meta, sources
from banklake.lineage import GOLD_LINEAGE, table_lineage
from banklake.masking import mask_value, masking_rules
from banklake.schemas import FIRST_BUSINESS_DATE, ORDER, SOURCES

from .. import helpers
from ..components import badge, fmt_int, strip, table

SOURCE_SYSTEM = "Finacle core banking (Teradata)"
EXAMPLE = {"NAME": "Aarav Mehta", "EMAIL": "aarav@example.com", "ACCOUNT": "123456789012", "PHONE": "9876543210"}
INK, MUTE, LINE = "#1F2937", "#6B7280", "#CBD2DC"
NAVY, GREEN, AMBER = "#172334", "#1F6B4F", "#B4690E"


def _has_data():
    return bool(meta.query("SELECT 1 FROM pipeline_execution_log LIMIT 1"))


@st.cache_data(ttl=4, show_spinner=False)
def counts():
    c = {"source": {}, "bronze": {}, "silver_merge": {}, "silver": {}, "gold": {}, "bad": {}}
    rows = meta.query("SELECT entity, records_read FROM ingestion_audit_log WHERE status!='SKIPPED' ORDER BY id")
    for r in rows:
        c["source"][r["entity"]] = c["source"].get(r["entity"], 0) + (r["records_read"] or 0)   # all loads so far, same basis as Bronze
    for e in ORDER:
        for layer, key in (("bronze", "bronze"), ("silver_merge", "silver_merge"), ("silver", "silver")):
            c[layer][e] = helpers.count(layer, e) if helpers.has(layer, e) else None
            q = "quarantine/" + layer + "_" + e
            c["bad"][(layer, e)] = helpers.count("quarantine", f"{layer}_{e}") if helpers.has("quarantine", f"{layer}_{e}") else 0
    for g in GOLD_LINEAGE:
        c["gold"][g] = helpers.count("gold", g) if helpers.has("gold", g) else None
    return c



def _none():
    st.markdown("#### No data in the lake yet")
    st.write("BankLake works on fictional banking data it generates itself. One click builds everything: it creates the source files, "
             "then runs all 23 pipelines from Bronze to Gold. It takes about two minutes.")
    a, b = st.columns(2)
    if a.button("Build the demo (generate data and run everything)", type="primary", width="stretch", key="empty_build"):
        helpers.cli_seq(("reset",), ("generate", "--scale", "small"), ("run",))
    b.caption("Prefer step by step? Use Actions in the top right: generate data first, then run all pipelines.")





def _go(page_key, label, key):
    from .. import nav
    if st.button(label, key=key, width="stretch", type="primary"):
        st.switch_page(nav.PAGES[page_key])


# ---------------------------------------------------------------- 1. the pipeline
def _svg(c):
    tot = lambda d: sum(v or 0 for v in d.values()) if d else 0  # noqa: E731
    src, br, mg, sv = tot(c["source"]), tot(c["bronze"]), tot(c["silver_merge"]), tot(c["silver"])
    gold_rows = tot(c["gold"])
    n_gold = sum(v is not None for v in c["gold"].values())
    bad = lambda layer: sum(v for (l_, _), v in c["bad"].items() if l_ == layer)  # noqa: E731
    W, H = 1240, 360
    o = [f'<svg viewBox="0 0 {W} {H}" width="100%" xmlns="http://www.w3.org/2000/svg" font-family="Inter, Arial, sans-serif">',
         f'<defs><marker id="ar" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="{MUTE}"/></marker></defs>']

    def group(x, w, title, color):
        o.append(f'<rect x="{x}" y="14" width="{w}" height="330" rx="8" fill="none" stroke="{color}" stroke-width="1.4" stroke-dasharray="5 4"/>'
                 f'<text x="{x + 16}" y="42" font-size="14" font-weight="600" fill="{color}" letter-spacing=".6">{title}</text>')

    def box(x, y, w, h, title, sub, n, fill, tag=None, extra=None):
        o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{fill}"/>'
                 f'<text x="{x + 14}" y="{y + 30}" font-size="17" font-weight="600" fill="#fff">{title}</text>'
                 f'<text x="{x + 14}" y="{y + 50}" font-size="12.5" fill="#E6EAF0">{sub}</text>'
                 f'<text x="{x + 14}" y="{y + h - 16}" font-size="16" font-weight="600" fill="#fff">{n}</text>')
        if tag:
            tw = 14 + 7.4 * len(tag)
            o.append(f'<rect x="{x + 14}" y="{y + 62}" width="{tw}" height="22" rx="11" fill="#fff" fill-opacity=".2"/>'
                     f'<text x="{x + 14 + tw / 2}" y="{y + 77}" font-size="11" font-weight="600" fill="#fff" text-anchor="middle" letter-spacing=".5">{tag}</text>')
        if extra:
            o.append(f'<text x="{x + 14}" y="{y + h - 40}" font-size="12" fill="#FFD9D4">{extra}</text>')

    def arrow(x1, x2, y):
        o.append(f'<line x1="{x1}" y1="{y}" x2="{x2 - 2}" y2="{y}" stroke="{MUTE}" stroke-width="1.8" marker-end="url(#ar)"/>')

    f = lambda v: "-" if not v else f"{v:,} rows"  # noqa: E731
    box(10, 100, 140, 160, "Source", "Finacle / Teradata", f(src), "#4B5563")
    arrow(150, 180, 180)
    group(180, 200, "1  INGESTION", NAVY)
    box(194, 100, 172, 160, "Bronze", "loaded, PII masked", f(br), NAVY, "MASKED", f"{bad('bronze'):,} set aside" if bad("bronze") else None)
    arrow(380, 410, 180)
    group(410, 560, "2  TRANSFORMATION", NAVY)
    box(424, 100, 160, 160, "Silver Merge", "changes applied", f(mg), NAVY, "MASKED")
    arrow(584, 610, 180)
    box(610, 100, 160, 160, "Silver Transform", "cleaned, validated", f(sv), NAVY, "MASKED", f"{bad('silver'):,} set aside" if bad("silver") else None)
    arrow(770, 796, 180)
    box(796, 100, 160, 160, "Gold", "business tables", f"{n_gold} tables", GREEN, "UNMASKED")
    arrow(970, 1000, 180)
    group(1000, 230, "3  REPORTING", GREEN)
    box(1014, 100, 202, 160, "BI reports", "dashboards for executives", f"reads {n_gold} Gold tables" if n_gold else "-", GREEN, "UNMASKED")
    o.append(f'<text x="280" y="304" font-size="12.5" fill="{MUTE}" text-anchor="middle">PII → masked value + token</text>')
    o.append(f'<text x="690" y="304" font-size="12.5" fill="{MUTE}" text-anchor="middle">source tables → Silver tables → Gold tables</text>')
    o.append(f'<text x="1115" y="304" font-size="12.5" fill="{MUTE}" text-anchor="middle">real values read back from the vault</text>')
    o.append("</svg>")
    return "".join(o)


def home():
    st.markdown("### Banking ETL pipeline")
    st.caption("Finacle (Teradata) data comes in, is masked at Bronze, is transformed through Silver into Gold, and is reported in BI. Synthetic data only.")
    if not _has_data():
        _none()
        return
    c = counts()
    st.image(_svg(c), width="stretch")
    a, b, d, e = st.columns(4)
    with a:
        _go("ingestion", "1  Ingestion", "h_ing")
    with b:
        _go("transformation", "2  Transformation", "h_tr")
    with d:
        _go("reporting", "3  Reporting and BI", "h_rep")
    with e:
        _go("validation", "Validation", "h_val")
    v = _validation_summary()
    if v:
        st.markdown(f"**Validation:** {badge(v['status'])} {v['passed']} of {v['total']} checks passed in the latest run ({v['run_id']}).", unsafe_allow_html=True)
    else:
        st.markdown(badge("NOT RUN") + " Validation has not been run yet. Open Validation to compare record counts with the source.", unsafe_allow_html=True)


# ---------------------------------------------------------------- 2. ingestion
ARROW = {"waiting": "#9CA3AF", "processing": "#D97706", "done": "#1F6B4F", "failed": "#B42318", "idle": "#CBD2DC"}
STAGE_NAME = {"extract": "Extraction", "transform": "Masking", "insert": "Bronze ingestion"}
STATE_WORD = {"waiting": "Waiting", "processing": "Processing", "done": "Completed", "failed": "Failed"}
esc = html.escape


def _pipeline_svg(snap, arrows, src_rows):
    """The ingestion pipeline. Arrow colours are the backend's stage states (ingest.arrow_states), not a timer."""
    tabs = (snap or {}).get("tables", [])
    ext = sum(t["extracted"] for t in tabs)
    trn = sum(t["transformed"] for t in tabs)
    ins = sum(t["inserted"] for t in tabs)
    o = ['<svg viewBox="0 0 1180 392" xmlns="http://www.w3.org/2000/svg" font-family="Inter,Segoe UI,Arial,sans-serif" text-rendering="geometricPrecision">']
    o.append("<defs>")
    for k, col in ARROW.items():
        o.append(f'<marker id="a_{k}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
                 f'<path d="M0,0 L10,5 L0,10 z" fill="{col}"/></marker>')
    o.append('<filter id="sh" x="-5%" y="-5%" width="110%" height="115%"><feDropShadow dx="0" dy="1" stdDeviation="2" flood-color="#172334" flood-opacity=".10"/></filter>')
    o.append("</defs>")

    def arrow(d, state):
        col = ARROW[state]
        extra = ' stroke-dasharray="9 6"><animate attributeName="stroke-dashoffset" from="30" to="0" dur="0.7s" repeatCount="indefinite"/></path>' \
            if state == "processing" else (' stroke-dasharray="3 5"></path>' if state == "idle" else "></path>")
        o.append(f'<path d="{d}" fill="none" stroke="{col}" stroke-width="3" marker-end="url(#a_{state})"{extra}')

    def card(x, y, w, h, tag, title, lines, accent):
        o.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="#FFFFFF" stroke="{LINE}" filter="url(#sh)"/>')
        o.append(f'<path d="M{x},{y + 10} a10,10 0 0 1 10,-10 h2 v{h} h-2 a10,10 0 0 1 -10,-10 z" fill="{accent}"/>')
        o.append(f'<text x="{x + 24}" y="{y + 26}" font-size="12.5" font-weight="700" letter-spacing=".1em" fill="#374151">{esc(tag)}</text>')
        o.append(f'<text x="{x + 24}" y="{y + 50}" font-size="16.5" font-weight="800" fill="{INK}">{esc(title)}</text>')
        for i, ln in enumerate(lines):
            o.append(f'<text x="{x + 24}" y="{y + 74 + i * 22}" font-size="14.5" font-weight="500" fill="#374151">{esc(ln)}</text>')

    sid_y = {}
    for i, sid in enumerate(SOURCES):
        cy = 62 + i * 118
        cx = 78
        sid_y[sid] = cy
        col = ARROW[arrows["S" + sid[-1]]]
        stroke = f'fill="#FFFFFF" stroke="{col}" stroke-width="3.5" stroke-linejoin="round" filter="url(#sh)"'
        g = f'fill="none" stroke="{INK}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"'
        if i == 0:      # hexagon + bank
            pts = " ".join(f"{cx + 46 * math.cos(math.radians(60 * k - 30)):.1f},{cy + 46 * math.sin(math.radians(60 * k - 30)):.1f}" for k in range(6))
            o.append(f'<polygon points="{pts}" {stroke}/>')
            o.append(f'<g {g}><path d="M{cx - 19},{cy - 4} L{cx},{cy - 19} L{cx + 19},{cy - 4} Z"/><path d="M{cx - 12},{cy} v14 M{cx},{cy} v14 M{cx + 12},{cy} v14 M{cx - 19},{cy + 18} h38"/></g>')
        elif i == 1:    # circle + person
            o.append(f'<circle cx="{cx}" cy="{cy}" r="44" {stroke}/>')
            o.append(f'<g {g}><circle cx="{cx}" cy="{cy - 10}" r="9"/><path d="M{cx - 18},{cy + 22} a18,15 0 0 1 36,0"/></g>')
        else:           # diamond + coin
            o.append(f'<rect x="{cx - 34}" y="{cy - 34}" width="68" height="68" rx="8" transform="rotate(45 {cx} {cy})" {stroke}/>')
            o.append(f'<g {g}><circle cx="{cx}" cy="{cy}" r="16"/><circle cx="{cx}" cy="{cy}" r="9"/><path d="M{cx},{cy - 4} v8"/></g>')
        o.append(f'<text x="144" y="{cy + 7}" font-size="21" font-weight="800" letter-spacing=".08em" fill="{INK}">{esc(sid)}</text>')
    for sid in SOURCES:
        arrow(f"M284,{sid_y[sid]} C340,{sid_y[sid]} 345,186 398,186", arrows["S" + sid[-1]])
    card(400, 108, 232, 156, "STAGE 1", "Extraction", ["Reads every table due", f"{ext:,} records extracted"], "#172334")
    arrow("M632,186 L688,186", arrows["transform"])
    card(690, 108, 262, 156, "STAGE 2", "Masking", ["TRIM + UPPER on flag columns", "AES-256 masking of PII", f"{trn:,} records masked"], "#172334")
    arrow("M952,186 L1002,186", arrows["insert"])
    card(1004, 108, 172, 156, "STAGE 3", "Bronze Ingestion", ["Complete", f"{ins:,} inserted"], "#172334")
    x = 24
    for k in ("waiting", "processing", "done", "failed"):
        o.append(f'<line x1="{x}" y1="376" x2="{x + 26}" y2="376" stroke="{ARROW[k]}" stroke-width="3"/>')
        o.append(f'<text x="{x + 34}" y="380" font-size="14" font-weight="500" fill="#374151">{STATE_WORD[k]}</text>')
        x += 130
    o.append("</svg>")
    return "".join(o)


@st.cache_resource(show_spinner="Preparing the source databases (first time only)...")
def _prepare():
    meta.init_db()
    sources.ensure_sources()
    return True


def _results(snap):
    rows = []
    for t in snap["tables"]:
        rows.append({"Source": t["source_id"], "Table": t["entity"], "Load frequency": t["frequency"].title(),
                     "Source records": t["source_count"], "Bronze records": t["bronze_count"], "Status": t["status"].title()})
    return pd.DataFrame(rows)


def _live_view():
    snap = ingest.current()
    arrows = ingest.arrow_states(snap)
    live = bool(snap) and snap["status"] == "RUNNING" and ingest.running()
    if snap and snap["status"] == "RUNNING" and not ingest.running():   # process restarted mid-run: do not claim success
        snap = dict(snap, status="FAILED", error="The ingestion process stopped before it finished.")
    with st.container(border=True):
        st.image(_pipeline_svg(snap, arrows, None), width="stretch")
    if snap is None:
        return live
    if live:
        tabs = snap["tables"]
        done = sum(t["status"] in ("SUCCESS", "FAILED", "SKIPPED") for t in tabs)
        strip([("Current source", snap["current_source"] or "-", ""), ("Current table", snap["current_table"] or "-", ""),
               ("Stage", STAGE_NAME.get(snap["current_stage"], "-"), f"{done} of {len(tabs)} tables finished"),
               ("Extracted", fmt_int(sum(t["extracted"] for t in tabs)), "records"),
               ("Masked", fmt_int(sum(t["transformed"] for t in tabs)), "records"),
               ("Inserted", fmt_int(sum(t["inserted"] for t in tabs)), "records")])
    else:
        _finished(snap)
    for t in snap["tables"]:
        if t["status"] == "FAILED":
            st.error(f"{t['source_id']} · {t['entity']} failed at the {t['failed_stage']} stage: {t['message']}")
    return live


def _finished(snap):
    tabs = snap["tables"]
    ok = sum(t["status"] == "SUCCESS" for t in tabs)
    bad = sum(t["status"] == "FAILED" for t in tabs)
    skip = sum(t["status"] == "SKIPPED" for t in tabs)
    if snap.get("error"):
        st.error(f"Run {snap['run_id']} did not finish: {snap['error']}")
    strip([("Business date", pd.Timestamp(snap["business_date"]).strftime("%d %b %Y"), snap["run_id"]),
           ("Tables loaded", ok, f"{skip} already loaded" if skip else ""), ("Tables failed", bad, ""),
           ("Source records", fmt_int(sum(t["source_count"] for t in tabs)), ""),
           ("Bronze records", fmt_int(sum(t["bronze_count"] or 0 for t in tabs)), "")])
    if not tabs:
        st.info("No tables were due on this business date.")
        return
    res = _results(snap)
    ev = st.dataframe(res, hide_index=True, width="stretch", height=min(38 + 35 * len(res), 560), on_select="rerun", selection_mode="single-row", key="ing_results",
                      column_config={"Source records": st.column_config.NumberColumn(format="localized"),
                                     "Bronze records": st.column_config.NumberColumn(format="localized")})
    sel = ev.selection.rows if ev and ev.selection else []
    if sel:
        _inspect(snap["tables"][sel[0]])


def _inspect(t):
    with st.container(border=True):
        st.markdown(f"**{t['entity']}** · {t['source_id']} · {t['frequency'].title()} load · batch {t['batch_id'] or '-'}")
        if t["status"] == "FAILED" or not t["batch_id"]:
            st.caption("This table was not loaded, so there are no Bronze records for this run.")
            return
        df = ingest.bronze_batch(t["entity"], t["batch_id"], 200)
        if df.empty:
            st.caption("This load contained no records.")
            return
        rules = ingest._rules(t["entity"])
        kinds = {c: ("AES-256 masked" if c in masking_rules(t["entity"]) else "Token" if c.endswith("_token") else "Metadata" if c.startswith("_")
                     else "TRIM + UPPER" if c in rules else "") for c in df.columns}
        c1, c2 = st.columns([1, 3])
        with c1:
            table(pd.DataFrame({"Column": list(kinds), "Bronze treatment": list(kinds.values())}), height=380)
        with c2:
            table(df, height=380)


def _controls():
    latest = sources.latest_business_date()
    c1, c2, c2b, c3 = st.columns([1.1, 1, 0.8, 2.1], vertical_alignment="bottom")
    bd = c1.date_input("Business date", value=date(2026, 7, 1), min_value=date.fromisoformat(FIRST_BUSINESS_DATE), max_value=date(2027, 12, 31), key="ing_date",
                       format="DD/MM/YYYY")
    future = bd > latest
    go = c2.button("Start Ingestion", type="primary", width="stretch", disabled=ingest.running() or future, key="ing_start")
    if c2b.button("Dry Run", width="stretch", disabled=future, key="ing_dry"):
        _dry_run_dialog(bd)
    snap = ingest.current()
    if snap:
        state = "RUNNING" if ingest.running() else snap["status"]
        c3.markdown(f"{badge(state)}&nbsp; <span style='color:#6B7280;font-size:.85rem'>{esc(snap['run_id'])} · "
                    f"{pd.Timestamp(snap['business_date']):%d %b %Y}</span>", unsafe_allow_html=True)
    if future:
        st.warning(f"Ingestion is available only up to {latest:%d %b %Y} (yesterday). {bd:%d %b %Y} is not available yet; choose an earlier business date.")
    if go:
        try:
            ingest.start(bd, pace=0.25)
        except ValueError as e:
            st.error(str(e))
            return
        st.rerun()


@st.dialog("Dry run: one row, step by step", width="large")
def _dry_run_dialog(bd):
    st.caption(f"Business date {bd:%d %b %Y}. A real record is taken from the source and put through each step. Nothing is written.")
    due = sources.due_loads(bd)
    if not due:
        st.info("No tables are due on this business date.")
        return
    labels = {f"{l.entity}  ·  {l.frequency.title()}": l.entity for l in due}
    c1, c2 = st.columns([3, 1], vertical_alignment="bottom")
    pick = c1.selectbox("Table", list(labels), key="dry_table")
    nth = c2.number_input("Row number", min_value=1, value=1, step=1, key="dry_row")
    if not st.button("Run dry run", type="primary", key="dry_go"):
        return
    try:
        steps = ingest.dry_run_steps(labels[pick], bd, int(nth))
    except ValueError as e:
        st.error(str(e))
        return
    ctx = {}
    for i, (title, fn) in enumerate(steps, 1):
        box = st.status(f"Step {i} of {len(steps)}  ·  {title}", expanded=True, state="running")
        time.sleep(0.7)
        try:
            with box:
                res = fn(ctx)
                st.write(res["note"])
                if res.get("sql"):
                    st.code(res["sql"], language="sql")
                if res.get("table") is not None:
                    st.dataframe(res["table"], hide_index=True, width="stretch", height=min(38 + 35 * len(res["table"]), 420))
            box.update(state="complete", expanded=True)
        except Exception as e:  # noqa: BLE001 - show the failing step and stop
            box.update(state="error", expanded=True)
            st.error(f"Step {i} failed: {e}")
            return
    st.success("Dry run complete. Nothing was written to Bronze or the vault.")


def ingestion():
    st.markdown("<div style='color:#172334;font-weight:800;font-size:2rem;line-height:1.2;margin:0 0 1rem 0;letter-spacing:-.01em'>Ingestion Pipeline</div>", unsafe_allow_html=True)
    _prepare()
    _controls()
    if ingest.running():
        _live_fragment()
    else:
        if st.session_state.pop("_ing_was_running", False):
            st.cache_data.clear()
        _live_view()


@st.fragment(run_every=0.5)
def _live_fragment():
    st.session_state["_ing_was_running"] = True
    if not _live_view():
        st.rerun()


# ---------------------------------------------------------------- 3. transformation
def _steps(c):
    rows = []
    for key, name in (("silver_merge", "Silver Merge"), ("silver", "Silver Transform")):
        for e in ORDER:
            prev = c["bronze"][e] if key == "silver_merge" else c["silver_merge"][e]
            rows.append({"step": name, "table": e, "input rows": prev, "output rows": c[key][e], "set aside": c["bad"][(key, e)]})
    return rows


def transformation():
    st.markdown("### 2  Transformation: Bronze to Silver to Gold")
    if not _has_data():
        _none()
        return
    c = counts()
    t1, t2, t3, t4 = st.tabs(["Silver Merge", "Silver Transform", "Gold", "Teradata → Athena → EMR"])
    steps = pd.DataFrame(_steps(c))
    with t1:
        st.caption("Reads Bronze and applies new, changed and deleted records, so there is one current row per key. Still masked.")
        table(steps[steps.step == "Silver Merge"].drop(columns="step"))
        e = st.selectbox("Preview", ORDER, key="sm_preview")
        if helpers.has("silver_merge", e):
            table(helpers.sample("silver_merge", e, 100))
    with t2:
        st.caption("Cleans values, checks business rules and joins tables. Rows that fail a rule go to quarantine with the reason. Still masked.")
        table(steps[steps.step == "Silver Transform"].drop(columns="step"))
        e = st.selectbox("Preview", ORDER, key="st_preview")
        if helpers.has("silver", e):
            table(helpers.sample("silver", e, 100))
        if helpers.has("quarantine", f"silver_{e}"):
            st.markdown(f"**Set aside from {e}**")
            table(helpers.Q(f"SELECT reject_reason, count(*) AS rows FROM {helpers.pq('quarantine', 'silver_' + e)} GROUP BY 1 ORDER BY 2 DESC"))
    with t3:
        st.caption("Gold tables are built from the Silver tables and are the first unmasked layer: customer_360 gets its real names, emails and phones back from the vault (audited).")
        lin = table_lineage()
        table(pd.DataFrame([{"Gold table": g, "rows": c["gold"].get(g), "built from Silver": ", ".join(lin.get(g, []))} for g in GOLD_LINEAGE]))
        g = st.selectbox("Preview a Gold table", list(GOLD_LINEAGE), key="gd_preview")
        if helpers.has("gold", g):
            table(helpers.sample("gold", g, 100))
    with t4:
        st.caption("The on-premise Teradata procedures were converted to Athena SQL, and EMR (PySpark) scripts were created to load the Gold tables.")
        from . import procs
        procs.render()
        with st.expander("Convert a single query instead"):
            from .. import legacy
            legacy.render("SQL migration", helpers.current_role())


# ---------------------------------------------------------------- 4. reporting
def reporting():
    st.markdown("### 3  Reporting: Gold tables and BI")
    if not _has_data():
        _none()
        return
    c = counts()
    t1, t2 = st.tabs(["Gold tables", "BI reports"])
    with t1:
        st.caption("Reporting reads only Gold. Gold holds unmasked (synthetic) customer details, so these tables stay on this machine.")
        table(pd.DataFrame([{"Gold table": g, "rows": c["gold"].get(g), "grain": GOLD_LINEAGE[g][0]} for g in GOLD_LINEAGE]))
        g = st.selectbox("Open a Gold table", list(GOLD_LINEAGE), key="rp_gold")
        if helpers.has("gold", g):
            table(helpers.sample("gold", g, 200))
    with t2:
        from .. import legacy
        legacy.render("BI dashboards", helpers.current_role())


# ---------------------------------------------------------------- 5. validation
def _validation_summary():
    last = meta.query("SELECT run_id FROM reconciliation_results ORDER BY id DESC LIMIT 1")
    if not last:
        return None
    r = meta.query("SELECT status, count(*) n FROM reconciliation_results WHERE run_id=? AND check_name NOT LIKE 'info_%' "
                   "AND check_name NOT IN ('rejection_breakdown','join_funnel') GROUP BY status", (last[0]["run_id"],))
    total, passed = sum(x["n"] for x in r), sum(x["n"] for x in r if x["status"] == "PASS")
    return dict(run_id=last[0]["run_id"], total=total, passed=passed, status="SUCCESS" if total and passed == total else "FAILED")


def validation():
    st.markdown("### Validation: do the record counts match the source?")
    st.caption("Counts and keys are compared between the source files (Finacle extract) and each BankLake layer.")
    if not _has_data():
        _none()
        return
    c = counts()
    rows = []
    for e in ORDER:
        rejected = c["bad"][("bronze", e)] + c["bad"][("silver_merge", e)] + c["bad"][("silver", e)]
        rows.append({"table": e, "source": c["source"].get(e), "Bronze": c["bronze"].get(e), "Silver Merge": c["silver_merge"].get(e),
                     "Silver Transform": c["silver"].get(e), "set aside": rejected})
    table(pd.DataFrame(rows))
    st.caption("Fewer rows after Bronze is expected: duplicates are merged, deleted records are removed, and rows that fail a rule are set aside with the reason.")
    if st.button("Run validation now", type="primary", key="val_run"):
        helpers.cli_seq(("reconcile",))
    v = _validation_summary()
    if not v:
        st.markdown(badge("NOT RUN") + " No validation run yet.", unsafe_allow_html=True)
        return
    st.markdown(f"{badge(v['status'])} {v['passed']} of {v['total']} checks passed (run {v['run_id']}).", unsafe_allow_html=True)
    res = pd.DataFrame(meta.query("SELECT stage, entity, check_name, source_value, target_value, status, detail FROM reconciliation_results "
                                  "WHERE run_id=? AND check_name NOT LIKE 'info_%' ORDER BY (status='PASS'), id", (v["run_id"],)))
    f = st.segmented_control("Show", ["Failures first", "All checks"], default="Failures first", key="val_filter")
    table(res if f == "All checks" else res[res.status != "PASS"] if (res.status != "PASS").any() else res.head(30), height=420)
