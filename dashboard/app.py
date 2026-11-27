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

st.set_page_config(page_title="BankLake", page_icon=":material/database:", layout="wide", initial_sidebar_state="collapsed")

from bankui import helpers, nav, theme  # noqa: E402
from bankui.views import etl, home  # noqa: E402
from banklake import meta  # noqa: E402

meta.init_db()
theme.apply()
st.logo(str(HERE / "assets" / "banklake_logo.svg"), size="large")

P = st.Page
pages = {
    "home": P(home.page, title="Home", icon=":material/home:", url_path="", default=True),
    "ingestion": P(etl.ingestion, title="Step 1: Ingestion", icon=":material/upload_file:", url_path="ingestion"),
    "transformation": P(etl.transformation, title="Step 2: Transformation", icon=":material/transform:", url_path="transformation"),
    "reporting": P(etl.reporting, title="Step 3: Reporting and BI", icon=":material/bar_chart:", url_path="reporting"),
    "validation": P(etl.validation, title="Step 4: Validation", icon=":material/fact_check:", url_path="validation"),
    "overview": P(etl.home, title="Pipeline Overview", icon=":material/account_tree:", url_path="overview"),
}
nav.PAGES.update(pages)
current = st.navigation(list(pages.values()), position="hidden")   # one navigation bar only: the step bar under the title

# ------------------------------------------------------------------ header: one navy bar, one row of tabs, compact Refresh
key = next((k for k, p in pages.items() if p is current), "home")
legacy_page = key in ("overview", "validation")   # these two still show the run strip and Actions
FLOW = ["home", "ingestion", "transformation", "reporting", "validation"]
STEP_LABEL = {"home": "Home", "ingestion": "Step 1: Ingestion", "transformation": "Step 2: Transformation", "reporting": "Step 3: Reporting and BI", "validation": "Step 4: Validation", "overview": "Pipeline Overview"}
with st.container(key="bl_head"):
    cols = st.columns([0.9, 0.7, 1.2, 1.5, 1.8, 1.25, 1.35, 0.75], gap="small", vertical_alignment="center")
    cols[0].markdown('<div class="bl-brand2"><svg width="26" height="26" viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg"><circle cx="16" cy="16" r="15" fill="#000" stroke="#fff" stroke-width="1.5"/><path d="M5 13 q5.5-5 11 0 t11 0" fill="none" stroke="#86BC25" stroke-width="2.4" stroke-linecap="round"/><path d="M5 19 q5.5-5 11 0 t11 0" fill="none" stroke="#fff" stroke-width="2.4" stroke-linecap="round"/><path d="M5 25 q5.5-5 11 0 t11 0" fill="none" stroke="#F58220" stroke-width="2.4" stroke-linecap="round"/></svg></div>', unsafe_allow_html=True)
    for col, (pk, label) in zip(cols[1:7], STEP_LABEL.items()):
        cls = "step_current" if pk == key else "step_other"
        if col.button(label, key=f"{cls}_{pk}", width="stretch") and pk != key:
            nav.open_page(pk)
    with cols[7]:
        if st.button("Refresh", key="bl_refresh", width="stretch", help="Reload the logs and tables"):
            st.cache_data.clear()
            st.rerun()

if legacy_page:
    _, actions_col = st.columns([9, 1.2], vertical_alignment="center", gap="small")
    with actions_col.popover("Actions", width="stretch"):
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

