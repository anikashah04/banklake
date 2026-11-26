"""BankLake: enterprise data platform console.   Run:  streamlit run dashboard/app.py

The console never runs Spark itself. Buttons launch `python main.py ...` as a subprocess; every view reads the
metadata database (SQLite) or the Parquet lake (DuckDB). Nothing shown here is hardcoded: statuses, counts and
timestamps all come from the execution logs and the lake. Features that are not built say so.
"""
import sys
from pathlib import Path

import streamlit as st

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

st.set_page_config(page_title="BankLake", page_icon=":material/database:", layout="wide", initial_sidebar_state="expanded")

from bankui import helpers, nav, state, theme  # noqa: E402
from bankui.components import badge  # noqa: E402
from bankui.views import etl, home  # noqa: E402
from banklake import meta  # noqa: E402

meta.init_db()
theme.apply()
st.logo(str(HERE / "assets" / "banklake_logo.svg"), size="large")

P = st.Page
pages = {
    "home": P(home.page, title="Home", icon=":material/home:", url_path="", default=True),
    "ingestion": P(etl.ingestion, title="1  Ingestion", icon=":material/upload_file:", url_path="ingestion"),
    "transformation": P(etl.transformation, title="2  Transformation", icon=":material/transform:", url_path="transformation"),
    "reporting": P(etl.reporting, title="3  Reporting and BI", icon=":material/bar_chart:", url_path="reporting"),
    "validation": P(etl.validation, title="Validation", icon=":material/fact_check:", url_path="validation"),
    "overview": P(etl.home, title="Pipeline Overview", icon=":material/account_tree:", url_path="overview"),
}
nav.PAGES.update(pages)
SECTIONS = {"": ["home"], "Pipeline": ["ingestion", "transformation", "reporting"], "Checks": ["validation", "overview"]}
current = st.navigation({k: [pages[x] for x in v] for k, v in SECTIONS.items()}, position="sidebar", expanded=True)

with st.sidebar:
    st.caption("Personal data is masked at Bronze and stays masked in every later layer.")
    st.caption("Synthetic data only. Independent academic project; no client data or code.")

# ------------------------------------------------------------------ header bar
SUBTITLE = {"home": "Pick up where you left off, or open any step.",
            "validation": "Check that record counts match between the source and every layer.",
            "ingestion": "Load one business day from the three source systems into Bronze. Personal data is masked here.",
            "transformation": "Turn Bronze into Silver and Gold tables for a report date.",
            "reporting": "Read the Gold tables as reports, dashboards and BI analysis."}
key = next((k for k, p in pages.items() if p is current), "home")
legacy_page = key in ("overview", "validation")   # these two still show the run header and Actions
hs = state.header_state()
bar, refresh_col, actions_col = st.columns([9, 1.1, 1.2] if legacy_page else [9, 1.1, 0.01], vertical_alignment="center", gap="small")
meta_html = ""
if legacy_page:
    last_ok = (hs["last_success"] or "none yet")
    meta_html = (f'<div class="m"><div><span class="k">Environment</span><span class="v">{state.environment()}</span></div>'
                 f'<div><span class="k">Run ID</span><span class="v">{hs["run_id"] or "-"}</span></div>'
                 f'<div><span class="k">Batch</span><span class="v">{hs["batch"] if hs["batch"] is not None else "-"}</span></div>'
                 f'<div><span class="k">Pipeline status</span><span class="v">{badge(hs["status"])}</span></div>'
                 f'<div><span class="k">Last successful run</span><span class="v">{last_ok}</span></div></div>')
sub = SUBTITLE.get(key)
bar.markdown(f'<div class="bl-top"><div class="t">{current.title.strip()}'
             f'{f"<div style=\'font-weight:400;font-size:.82rem;color:#6B7280;margin-top:.1rem\'>{sub}</div>" if sub else ""}</div>{meta_html}</div>',
             unsafe_allow_html=True)
if refresh_col.button("Refresh", width="stretch", help="Reload the logs and tables"):
    st.cache_data.clear()
    st.rerun()
if legacy_page:
    with actions_col.popover("Actions", width="stretch"):
        st.caption("Each action runs the real command (python main.py ...) and refreshes the console.")
        scale = st.radio("Sample data size", ["small", "full"], horizontal=True, key="act_scale", format_func=lambda x: "Small (~1,000 customers)" if x == "small" else "Full (10,000 customers)")
        if st.button("Generate sample data (resets the lake)", key="act_gen", width="stretch"):
            helpers.cli_seq(("reset",), ("generate", "--scale", scale))
        if st.button("Run all pipelines", key="act_run", width="stretch", type="primary"):
            helpers.cli_seq(("run",))
        if st.button("Add incremental batch", key="act_inc", width="stretch"):
            helpers.cli_seq(("generate", "--incremental"))
        if st.button("Add incremental batch with deletes", key="act_incd", width="stretch"):
            helpers.cli_seq(("generate", "--incremental", "--deletes"))
        if st.button("Run reconciliation", key="act_rec", width="stretch"):
            helpers.cli_seq(("reconcile",))

if "_last_action" in st.session_state:
    rc, cmd, out = st.session_state.pop("_last_action")
    (st.success if rc == 0 else st.error)(f"Finished: {cmd}" if rc == 0 else f"Finished with problems: {cmd}")
    with st.expander("Command output", expanded=(rc != 0)):
        st.code(out)

current.run()

# ------------------------------------------------------------------ the same way forward and back on every page
FLOW = ["home", "ingestion", "transformation", "reporting", "validation"]
if key == "overview":
    st.divider()
    if st.columns([1, 2])[0].button("Home", key="nav_home", width="stretch"):
        nav.open_page("home")
elif key in FLOW and key != "home":
    i = FLOW.index(key)
    st.divider()
    back, mid, fwd = st.columns([1, 1, 1])
    if back.button(f"← {pages[FLOW[i - 1]].title.strip()}", key="nav_back", width="stretch"):
        nav.open_page(FLOW[i - 1])
    if mid.button("Home", key="nav_home", width="stretch"):
        nav.open_page("home")
    if i < len(FLOW) - 1 and fwd.button(f"{pages[FLOW[i + 1]].title.strip()} →", key="nav_next", width="stretch"):
        nav.open_page(FLOW[i + 1])
