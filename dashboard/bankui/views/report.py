"""Reporting page: pick a report, see its Gold tables, the numbers and a chart for one report date.
Everything shown is read from the Gold tables the Transformation page loaded and from the audit log; nothing is estimated."""
from datetime import date

import pandas as pd
import streamlit as st

from banklake import meta
from banklake.transform import catalog, engine

from ..components import badge, table
from . import charts
from .etl import ARROW, INK, MUTE, esc
from .report_specs import SPECS

BAD = ("FAILED", "BLOCKED")
SKIP = {"report_date", "run_id", "business_date", "processed_date", "last_upd_date", "source_name"}
MEAN_WORDS = ("avg", "average", "rate", "ratio", "score", "pct", "percent")


# ------------------------------------------------------------------ report catalogue (from the Gold report list)
def reports():
    """{report name: dict(type, tables)} read from gold_report_usage, in name order."""
    out = {}
    for r in meta.query("SELECT report_name, report_type, target_table FROM gold_report_usage ORDER BY report_name, target_table"):
        out.setdefault(r["report_name"], dict(type=r["report_type"], tables=[]))["tables"].append(r["target_table"])
    return out


def table_state(t, rd):
    """ready / failed / waiting for one Gold table on the report date, plus the loaded row count."""
    last = meta.query("SELECT status FROM layer_audit_log WHERE layer='gold' AND target_table_name=? AND report_date=? ORDER BY id DESC LIMIT 1", (t, rd))
    ld = engine.loaded(t, "gold", rd)
    if last and last[0]["status"] in BAD:
        return "failed", None
    return ("done", ld["output_count"]) if ld else ("waiting", None)


def frame(t, rd, n=100000):
    df = engine.preview("gold", t, rd, n)
    return df if df is not None else pd.DataFrame()


def measures(df):
    """Numeric columns worth totalling (processing columns excluded)."""
    return [c for c in df.columns if c not in SKIP and pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_bool_dtype(df[c])]


def money(v):
    """Indian digit grouping: lakh and crore."""
    a = abs(v)
    return f"₹{v / 1e7:,.2f} Cr" if a >= 1e7 else f"₹{v / 1e5:,.2f} L" if a >= 1e5 else f"₹{v:,.0f}"


def value(df, col, how, fmt):
    s = df[col]
    v = s.nunique() if how == "nunique" else s.mean() if how == "mean" else s.max() if how == "max" else s.min() if how == "min" else s.sum()
    return money(v) if fmt == "inr" else f"{v:,.2f}%" if fmt == "pct" else f"{v:,.2f}" if fmt == "dec" else f"{v:,.0f}"


def dims_and_measures(df):
    """Columns people can break down by, and columns they can total. Ids, tokens, masked PII and processing columns are left out."""
    hide = SKIP | {"customer_id", "customer_name", "email", "phone", "branch_id", "latest_rate_date"}
    dims, meas = [], []
    for c in df.columns:
        if c in hide or c.endswith("_token"):
            continue
        if pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_bool_dtype(df[c]):
            (dims if c.endswith(("_year", "_month")) else meas).append(c)
        else:
            dims.append(c)
    return dims, meas


# ------------------------------------------------------------------ the strip: Gold tables -> report -> output
def _svg(name, info, rd, states):
    tabs = info["tables"]
    worst = "failed" if any(s == "failed" for s, _ in states.values()) else "done" if all(s == "done" for s, _ in states.values()) else "waiting"
    o = ['<svg viewBox="0 0 1240 150" xmlns="http://www.w3.org/2000/svg" font-family="Times New Roman,Times,serif" text-rendering="geometricPrecision"><defs>']
    for k, col in ARROW.items():
        o.append(f'<marker id="r_{k}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{col}"/></marker>')
    o.append("</defs>")

    def box(x, w, tag, title, lines, col):
        o.append(f'<rect x="{x}" y="14" width="{w}" height="112" rx="10" fill="#FFFFFF" stroke="{col}" stroke-width="2"/>')
        o.append(f'<text x="{x + 16}" y="38" font-size="12" font-weight="700" letter-spacing=".08em" fill="#374151">{esc(tag)}</text>')
        o.append(f'<text x="{x + 16}" y="62" font-size="16" font-weight="800" fill="{INK}">{esc(title)}</text>')
        for i, ln in enumerate(lines[:3]):
            o.append(f'<text x="{x + 16}" y="{82 + i * 17}" font-size="12.5" fill="#374151">{esc(ln)}</text>')

    def arrow(x1, x2, st_):
        o.append(f'<path d="M{x1},70 L{x2},70" fill="none" stroke="{ARROW[st_]}" stroke-width="3" marker-end="url(#r_{st_})"/>')

    lines = [t.replace("gold_", "") + (f"  ·  {states[t][1]:,} rows" if states[t][1] is not None else "  ·  not loaded") for t in tabs[:2]]
    if len(tabs) > 2:
        lines.append(f"+ {len(tabs) - 2} more")
    box(14, 400, "GOLD TABLES", f"{len(tabs)} table{'s' if len(tabs) != 1 else ''}", lines, ARROW[worst])
    arrow(414, 470, worst)
    box(472, 400, "REPORT", name if len(name) <= 30 else name[:29] + "…", [info["type"], f"report date {date.fromisoformat(rd):%d %b %Y}"], ARROW[worst])
    arrow(872, 928, worst)
    box(930, 296, "OUTPUT", "Preview and CSV", ["numbers, chart, table", "read from Gold only"], ARROW[worst])
    o.append("</svg>")
    return "".join(o)


# ------------------------------------------------------------------ details, loaded only when requested
@st.dialog("Report details", width="large")
def _details(name, info, rd):
    tabs = info["tables"]
    st.markdown(f"**{name}**  ·  {info['type']}  ·  report date {date.fromisoformat(rd):%d %b %Y}")
    t = st.tabs(["Gold Tables", "Lineage", "SQL", "Data", "Data Quality", "Audit Log"])
    with t[0]:
        rows = []
        for g in tabs:
            s, n = table_state(g, rd)
            ld = engine.loaded(g, "gold", rd)
            rows.append({"Gold table": g, "Status": {"done": "Completed", "failed": "Failed", "waiting": "Waiting"}[s], "Rows": n,
                         "Loaded at": ld["timestamp"] if ld else None})
        table(pd.DataFrame(rows), height=min(38 + 35 * len(rows), 300))
    with t[1]:
        rows = []
        for g in tabs:
            pid = f"gold.{g}"
            rows.append({"Gold table": g, "Silver tables": ", ".join(catalog.BY_ID[pid]["sources"]),
                         "Full chain": " → ".join(q["target"] for q in catalog.upstream(pid)) or "-"})
        table(pd.DataFrame(rows), height=min(38 + 35 * len(rows), 300))
    with t[2]:
        for g in tabs:
            art = engine.artifacts(f"gold.{g}") or {}
            st.markdown(f"`{g}`")
            if art.get("athena_status") == "CONVERTED":
                st.code(art["athena_sql"] + ";", language="sql", height=260)
            else:
                st.caption("Not converted yet. Open the Transformation page and run it.")
    with t[3]:
        for g in tabs:
            st.markdown(f"`{g}`")
            df = frame(g, rd, 500)
            table(df.drop(columns=[c for c in SKIP if c in df.columns]), height=min(35 * (min(len(df), 8) + 1) + 3, 300)) if not df.empty else st.caption("No rows for this date.")
    with t[4]:
        for g in tabs:
            df = frame(g, rd)
            st.markdown(f"`{g}`")
            if df.empty:
                st.caption("No rows for this date.")
                continue
            checks = [{"Check": "Rows loaded", "Result": f"{len(df):,}"},
                      {"Check": "Duplicate rows", "Result": f"{int(df.duplicated().sum()):,}"},
                      {"Check": "Columns with empty values", "Result": ", ".join(f"{c} ({int(n)})" for c, n in df.isna().sum().items() if n) or "none"}]
            pii = catalog.GOLD_INFO[g][3]
            if pii:
                un = sum(int((~df[c].astype(str).str.contains(r"\*", regex=True)).sum()) for c in pii if c in df.columns)
                checks.append({"Check": "PII values that are not masked", "Result": f"{un:,}"})
            table(pd.DataFrame(checks), height=38 + 35 * len(checks))
    with t[5]:
        rows = [r for r in engine.audit_rows(layer="gold", limit=400) if r["target_table_name"] in tabs][:60]
        table(pd.DataFrame(rows), height=340) if rows else st.caption("No audit rows for these tables yet.")


# ------------------------------------------------------------------ page
def _none():
    st.markdown("#### No data in the lake yet")
    st.write("Reports read the Gold tables. Run Ingestion, then the Transformation page, then come back here.")


def _frames(tabs, rd):
    return {t: frame(t, rd) for t in tabs}


def _plot(spec, frames, key):
    f = frames[spec["table"]]
    kw = {k: v for k, v in spec.items() if k not in ("table", "chart", "title")}
    ch = charts.build(spec["chart"], f, title=None, **kw)
    if ch is not None:
        st.markdown(f"<div style='font-weight:700;font-size:.92rem;color:{INK};margin:.2rem 0 .5rem 0'>{esc(spec['title'])}</div>", unsafe_allow_html=True)
        charts.render(ch, key)


def _spark(series):
    """Mini bar chart for a card: one bar per row of the Gold table (at most 14), same blue as the charts."""
    vals = [float(x) for x in series.dropna().tolist()][:14]
    if len(vals) < 5 or max(vals) <= 0:   # a handful of bars looks like blobs
        return ""
    hi = max(vals)
    w = 100 / len(vals)
    bars = "".join(f"<rect x='{i * w + 1:.1f}' y='{22 - 22 * max(v, 0) / hi:.1f}' width='{w - 2:.1f}' height='{22 * max(v, 0) / hi:.1f}' rx='1.5' fill='#257de0' opacity='{0.35 + 0.65 * v / hi:.2f}'/>"
                   for i, v in enumerate(vals))
    return f"<svg viewBox='0 0 100 22' preserveAspectRatio='none' style='width:100%;height:26px;margin-top:.35rem'>{bars}</svg>"


def _tiles(sp, frames):
    """KPI cards: the figure, what it is made of, and a mini bar chart of the rows behind it."""
    k = sp.get("kpis", [])
    if not k:
        return
    for col, (label, t, c, how, fmt) in zip(st.columns(len(k)), k):
        df = frames[t]
        n = len(df)
        note = {"sum": "total", "mean": "average", "max": "highest", "min": "lowest", "nunique": "distinct"}.get(how, "total")
        spark = _spark(df[c]) if how != "nunique" and pd.api.types.is_numeric_dtype(df[c]) else ""
        col.markdown(f"<div class='bl-kpi'><div class='k'>{esc(label)}</div><div class='v'>{value(df, c, how, fmt)}</div>"
                     f"<div class='s'>{note} across {n:,} row{'s' if n != 1 else ''}</div>{spark}</div>", unsafe_allow_html=True)


def _grid(sp, frames, name):
    ps = sp["panels"]
    for i in range(0, len(ps), 2):
        for j, col in enumerate(st.columns(2)):
            if i + j < len(ps):
                with col, st.container(border=True):
                    _plot(ps[i + j], frames, f"pl_{name}_{i + j}")


def _main_table(sp, frames, totals=True):
    tspec = sp["table"]
    df = frames[tspec["table"]][tspec["cols"]].copy()
    df = df.sort_values(tspec["sort"], ascending=tspec.get("ascending", False))
    shown = df.head(tspec["top"]) if tspec.get("top") else df
    if totals:
        tot = {c: None for c in df.columns}   # None, not "", so numeric columns stay numeric for the table
        first = next((c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])), df.columns[0])
        tot[first] = "TOTAL (all rows)"
        for c in df.columns:
            if pd.api.types.is_numeric_dtype(df[c]) and not any(w in c.lower() for w in MEAN_WORDS + ("year", "month")):
                tot[c] = round(float(df[c].sum()), 2)
        shown = pd.concat([pd.DataFrame([tot]), shown.astype(object)], ignore_index=True)
    return df, shown


def _dashboard(sp, frames, name, rd):
    _tiles(sp, frames)
    _grid(sp, frames, name)


def _report(sp, frames, name, rd):
    _tiles(sp, frames)
    with st.container(border=True):
        _plot(sp["panels"][0], frames, f"pl_{name}_0")
    df, shown = _main_table(sp, frames)
    cap = f"Top {sp['table']['top']} of {len(df):,} rows by {sp['table']['sort'].replace('_', ' ')}, with the total of all rows on the first line." if sp["table"].get("top") else \
        "All rows, with the total on the first line."
    st.caption(cap)
    table(shown, height=min(35 * (min(len(shown), 12) + 1) + 3, 460))
    st.download_button("Export CSV", df.to_csv(index=False), f"{sp['table']['table']}_{rd}.csv", "text/csv", key=f"rp_csv_{name}")


def _regulatory(sp, frames, name, rd):
    t = sp["table"]["table"]
    df, shown = _main_table(sp, frames)
    ld = engine.loaded(t, "gold", rd)
    sh = sp["share"]
    mix = frames[sh["table"]].groupby(sh["by"])[sh["measure"]].sum().sort_values(ascending=False)
    total = float(mix.sum())
    cols = st.columns(min(len(mix), 4) + 1)
    cols[0].metric("Records covered", f"{total:,.0f}")
    for col, (k, v) in zip(cols[1:], mix.head(4).items()):
        col.metric(f"Share {str(k).lower()}", f"{v / total:.1%}" if total else "-")
    with st.container(border=True):
        _plot(sp["panels"][0], frames, f"pl_{name}_0")
    st.markdown("**Return table**")
    table(shown, height=min(35 * (min(len(shown), 12) + 1) + 3, 460))
    ok = ld is not None and ld["output_count"] == len(df)
    st.markdown(f"{badge('PASS' if ok else 'FAIL')}&nbsp; Rows in this return ({len(df):,}) "
                f"{'match' if ok else 'do not match'} the rows loaded into <code>{esc(t)}</code> ({(ld or {}).get('output_count', 0):,}).", unsafe_allow_html=True)
    st.download_button("Export CSV", df.to_csv(index=False), f"{t}_{rd}.csv", "text/csv", key=f"rp_csv_{name}")


ROLES = {  # chart type -> [(role, label, kind of column)]
    "Column": [("x", "Category", "d"), ("y", "Measure", "m")], "Bar": [("x", "Category", "d"), ("y", "Measure", "m")],
    "Funnel": [("x", "Category", "d"), ("y", "Measure", "m")], "Waterfall": [("x", "Category", "d"), ("y", "Measure", "m")],
    "Donut": [("x", "Slices", "d"), ("y", "Measure", "m")], "Area": [("x", "X axis", "d"), ("y", "Measure", "m")],
    "Line": [("x", "X axis", "d"), ("y", "Measure", "m"), ("color", "Split by", "o")],
    "Stacked column": [("x", "X axis", "d"), ("y", "Measure", "m"), ("color", "Split by", "d")],
    "100% stacked": [("x", "X axis", "d"), ("y", "Measure", "m"), ("color", "Split by", "d")],
    "Grouped column": [("x", "X axis", "d"), ("y", "Measure", "m"), ("color", "Split by", "d")],
    "Stacked area": [("x", "X axis", "d"), ("y", "Measure", "m"), ("color", "Split by", "d")],
    "Scatter": [("x", "X measure", "m"), ("y", "Y measure", "m"), ("color", "Colour by", "o")],
    "Bubble": [("x", "X measure", "m"), ("y", "Y measure", "m"), ("size", "Size", "m"), ("color", "Colour by", "o")],
    "Heatmap": [("x", "Columns", "d"), ("y", "Rows", "d"), ("v", "Value", "m")],
    "Histogram": [("x", "Measure", "m")], "Box plot": [("x", "Category", "d"), ("y", "Measure", "m")],
    "Range": [("x", "Category", "d"), ("y", "Low", "m"), ("y2", "High", "m")],
    "Lollipop": [("x", "Category", "d"), ("y", "Measure", "m")], "Pie": [("x", "Slices", "d"), ("y", "Measure", "m")],
    "Treemap": [("x", "Boxes", "d"), ("y", "Size", "m"), ("color", "Group by", "o")], "Sunburst": [("x", "Rings", "d"), ("y", "Size", "m"), ("color", "Group by", "o")],
    "Radar": [("x", "Spokes", "d"), ("y", "Measure", "m"), ("color", "Split by", "o")], "Gauge": [("y", "Measure", "m"), ("y2", "Out of", "m")],
    "3D Scatter": [("x", "X measure", "m"), ("y", "Y measure", "m"), ("z", "Z measure", "m"), ("color", "Colour by", "o")],
    "3D Bubble": [("x", "X measure", "m"), ("y", "Y measure", "m"), ("z", "Z measure", "m"), ("size", "Size", "m"), ("color", "Colour by", "o")],
    "3D Line": [("x", "X axis", "d"), ("z", "Height", "m"), ("color", "Series", "o")],
    "3D Column": [("x", "X axis", "d"), ("y", "Y axis", "d"), ("z", "Height", "m")], "3D Surface": [("x", "X axis", "d"), ("y", "Y axis", "d"), ("z", "Height", "m")],
}
TOP_KINDS = ("Bar", "Funnel", "Donut", "Column", "Waterfall", "Lollipop", "Pie")


def _bi(sp, frames, name, rd):
    tabs = sp["tables"]
    pre = sp["preset"]
    dim_of = {k: "3D" if k in charts.KINDS_3D else "2D" for k in charts.ALL_KINDS}
    cols0 = st.columns([1.5, 1.0, 1.6, 1.4], vertical_alignment="bottom")
    t = cols0[0].selectbox("Table", tabs, key=f"bi_t_{name}") if len(tabs) > 1 else tabs[0]
    if len(tabs) == 1:
        cols0[0].markdown(f"<div style='font-size:.8rem;color:#6B7280'>Table</div><code>{tabs[0]}</code>", unsafe_allow_html=True)
    with cols0[1]:
        st.caption("Chart family")
        fam = st.segmented_control("Chart family", ["2D", "3D"], default=dim_of[pre["chart"]], key=f"bi_f_{name}", label_visibility="collapsed") or dim_of[pre["chart"]]
    kinds = charts.KINDS_3D if fam == "3D" else charts.KINDS_2D
    kind = cols0[2].selectbox(f"Chart type  ({len(kinds)} in {fam})", kinds, index=kinds.index(pre["chart"]) if pre["chart"] in kinds else 0, key=f"bi_k_{name}_{fam}")
    df = frames[t]
    dims, meas = dims_and_measures(df)
    roles = ROLES[kind]
    cols = st.columns(len(roles) + 1, vertical_alignment="bottom")
    pick, used_d, used_m = {}, 0, 0
    for col, (role, label, typ) in zip(cols, roles):
        opts = (meas if typ == "m" else dims if typ == "d" else ["(none)"] + dims)
        if not opts or opts == ["(none)"]:
            continue
        pre_v = pre.get(role) if t == tabs[0] and kind == pre["chart"] else None
        free = [i for i, o in enumerate(opts) if o not in pick.values()]
        idx = opts.index(pre_v) if pre_v in opts else 0 if typ == "o" else (free[0] if free else 0)
        v = col.selectbox(label, opts, index=idx, key=f"bi_{role}_{name}_{t}_{kind}")
        pick[role] = None if v == "(none)" else v
        used_m += typ == "m"
        used_d += typ == "d"
    how = cols[-1].selectbox("Total as", ["sum", "mean", "max", "min", "count"], key=f"bi_how_{name}_{kind}")
    need = [r for r, _, typ in roles if typ in ("m", "d")]
    if any(pick.get(r) is None for r in need):
        st.info("This table has no suitable columns for that chart type. Try another chart type.")
        return
    chosen = [pick[r] for r in pick if pick[r] is not None]
    if len(chosen) != len(set(chosen)):
        st.info("Each choice needs a different column. Change one of them.")
        return
    with st.container(border=True):
        ch = charts.build(kind, df, how=how, top=12 if kind in TOP_KINDS else None, **pick)
        if ch is not None:
            charts.render(ch, f"bi_chart_{name}_{t}_{kind}")
    st.caption(f"{len(df):,} rows in {t}")
    st.download_button("Export CSV", df.drop(columns=[c for c in SKIP if c in df.columns]).to_csv(index=False), f"{t}_{rd}.csv", "text/csv", key=f"rp_csv_{name}")


LAYOUT = {"Dashboard": _dashboard, "Report": _report, "Regulatory": _regulatory, "BI": _bi}


@st.fragment(run_every=0.5)
def _progress():
    run = engine.current()
    if run and engine.running():
        its = run["items"]
        st.session_state["_rp_was_running"] = True
        st.info(f"Loading the report tables: {sum(i['status'] in ('SUCCESS', 'FAILED', 'BLOCKED') for i in its)} of {len(its)} finished"
                f"{' · now ' + run['current'] if run['current'] else ''}")
    else:
        st.rerun()


def _last_error(t, rd):
    r = meta.query("SELECT error_message FROM layer_audit_log WHERE layer='gold' AND target_table_name=? AND report_date=? ORDER BY id DESC LIMIT 1", (t, rd))
    return (r[0]["error_message"] or "") if r else ""


def page():
    from banklake import ingest

    from .. import flow
    meta.init_db()
    if not flow.has_bronze():
        flow.no_data()
        return
    cat = reports()
    busy = engine.running()
    c1, c2, c3, c4 = st.columns([1.1, 3.0, 1.3, 1.0], vertical_alignment="bottom")
    with c1:
        bd = flow.pick_date("Report date", "rp_date")
    rd = bd.isoformat()
    nodata = {n for n, v in cat.items() if any(table_state(t, rd)[1] == 0 for t in v["tables"])}
    name = c2.selectbox(f"Report  ({len(cat)})", list(cat), format_func=lambda n: f"{n}   ·   {cat[n]['type']}" + ("   ·   no data" if n in nodata else ""), key="rp_report")
    info = cat[name]
    states = {t: table_state(t, rd) for t in info["tables"]}
    ready = all(s == "done" for s, _ in states.values())
    stale = flow.ingested_through() is None or bd > flow.ingested_through()
    load_b = c3.button("Load Report Data", type="primary", width="stretch", key="rp_load", disabled=ready or busy or stale or ingest.running(),
                       help="Run the Gold tables this report needs (and anything they depend on) for this date")
    det_b = c4.button("View Details", width="stretch", key="rp_details")
    if load_b:
        try:
            engine.start("gold", rd, info["tables"])
        except ValueError as e:
            st.error(str(e))
            return
        st.rerun()
    if det_b:
        _details(name, info, rd)
    with st.container(border=True):
        st.image(_svg(name, info, rd, states), width="stretch")
    failed = [t for t, (s, _) in states.items() if s == "failed"]
    with st.container(border=True):
        a, b = st.columns([4, 1.4], vertical_alignment="center")
        a.markdown(f"<div style='font-weight:800;font-size:1.15rem;color:{INK}'>{esc(name)}</div>"
                   f"<div style='color:{MUTE};font-size:.85rem'>{esc(info['type'])} · "
                   + ", ".join(f"<code>{esc(t)}</code>" for t in info["tables"]) + "</div>", unsafe_allow_html=True)
        empties = [t for t, (s_, n_) in states.items() if s_ == "done" and n_ == 0]
        b.markdown(f"<div style='text-align:right'>{badge('NO DATA' if ready and empties else 'READY' if ready else 'FAILED' if failed else 'NOT READY')}</div>", unsafe_allow_html=True)
    if busy:
        _progress()
        return
    if st.session_state.pop("_rp_was_running", False):
        st.cache_data.clear()
        st.rerun()
    if stale and not ready:
        flow.not_ingested(bd)
        return
    if failed:
        st.error("Could not load: " + "; ".join(f"{t} ({_last_error(t, rd) or 'see the Transformation page'})" for t in failed))
        flow.go("Open Transformation", "transformation", "rp_to_tr")
        return
    if not ready:
        st.info("This report has not been loaded for this date. Press Load Report Data.")
        return
    if empties:
        st.warning(f"No rows in {', '.join(empties)} for {bd:%d %b %Y}, so there is nothing to show. Bronze only holds the records of the days that were loaded one at a time. "
                   "Load everything up to this date with Run Entire Pipeline on Home, or Load History on Ingestion.")
        a_, b_, _ = st.columns([1.2, 1.2, 3])
        with a_:
            flow.go("Go to Home", "home", "rp_empty_home", primary=True)
        with b_:
            flow.go("Open Ingestion", "ingestion", "rp_empty_ing")
        return
    thin = [t for t, (s_, n_) in states.items() if n_ == 1]
    if thin:
        st.info(f"Only 1 row in {', '.join(thin)}. Bronze holds a single day for this table, so the figures are tiny. Run Entire Pipeline on Home for the full picture.")
    frames = _frames(info["tables"], rd)
    LAYOUT[info["type"]](SPECS[name], frames, name, rd)
