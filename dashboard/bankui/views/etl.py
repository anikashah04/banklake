"""The five pages of the console: the ETL pipeline, and one page per part of it. Every number is read from the lake or the logs."""
import pandas as pd
import streamlit as st

from banklake import meta
from banklake.lineage import GOLD_LINEAGE, table_lineage
from banklake.masking import mask_value, masking_rules
from banklake.schemas import ORDER

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
def ingestion():
    st.markdown("### 1  Ingestion: source to Bronze")
    st.caption(f"Source system: {SOURCE_SYSTEM}. Files are read as received, personal details are masked, and the result is stored as Bronze.")
    if not _has_data():
        _none()
        return
    c = counts()
    rows = []
    for e in ORDER:
        rows.append({"table": e, "source records": c["source"].get(e), "Bronze rows": c["bronze"].get(e), "set aside": c["bad"][("bronze", e)]})
    strip([("Source tables", len(ORDER), ""), ("Source records", fmt_int(sum(r["source records"] or 0 for r in rows)), "all loads"),
           ("Bronze rows", fmt_int(sum(r["Bronze rows"] or 0 for r in rows)), "masked"), ("Set aside", fmt_int(sum(r["set aside"] for r in rows)), "unreadable lines")])
    table(pd.DataFrame(rows))
    st.markdown("**PII masked at Bronze**")
    rules = [{"table": e, "column": col, "rule": r, "example": f"{EXAMPLE.get(r, '')} → {mask_value(r, EXAMPLE[r])}" if r in EXAMPLE else ""}
             for e in ORDER for col, r in masking_rules(e).items()]
    table(pd.DataFrame(rules))
    st.caption("The real value is encrypted (AES-256-GCM) into a separate vault. Bronze keeps the masked value and a token. The example column is an illustration, not a real record.")
    e = st.selectbox("Preview a Bronze table", ORDER, key="ing_preview")
    if helpers.has("bronze", e):
        table(helpers.sample("bronze", e, 100))


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
