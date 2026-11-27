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
current = st.navigation(list(pages.values()), position="hidden")   # one navigation bar only: the step bar under the title

# ------------------------------------------------------------------ header: one navy bar, one row of tabs, compact Refresh
key = next((k for k, p in pages.items() if p is current), "home")
legacy_page = key in ("overview", "validation")   # these two still show the run strip and Actions
FLOW = ["home", "ingestion", "transformation", "reporting", "validation"]
STEP_LABEL = {"home": "Home", "ingestion": "1  Ingestion", "transformation": "2  Transformation", "reporting": "3  Reporting and BI", "validation": "Validation", "overview": "Pipeline Overview"}
following = FLOW[FLOW.index(key) + 1] if key in FLOW and key != "home" and FLOW.index(key) < len(FLOW) - 1 else None
with st.container(key="bl_head"):
    cols = st.columns([1.1, 0.7, 1.15, 1.35, 1.7, 0.95, 1.35, 0.7], gap="small", vertical_alignment="center")
    cols[0].markdown('<div class="bl-brand2">BANKLAKE</div>', unsafe_allow_html=True)
    for col, (pk, label) in zip(cols[1:7], STEP_LABEL.items()):
        cls = "step_current" if pk == key else "step_next" if pk == following else "step_other"
        if col.button(label, key=f"{cls}_{pk}", width="stretch", help="Next step" if pk == following else None) and pk != key:
            nav.open_page(pk)
    with cols[7]:
        if st.button("Refresh", key="bl_refresh", width="stretch", help="Reload the logs and tables"):
            st.cache_data.clear()
            st.rerun()

if legacy_page:
    hs = state.header_state()
    last_ok = (hs["last_success"] or "none yet")
    m_col, actions_col = st.columns([9, 1.2], vertical_alignment="center", gap="small")
    m_col.markdown(f'<div class="bl-meta"><div><span class="k">Environment</span><span class="v">{state.environment()}</span></div>'
                   f'<div><span class="k">Run ID</span><span class="v">{hs["run_id"] or "-"}</span></div>'
                   f'<div><span class="k">Batch</span><span class="v">{hs["batch"] if hs["batch"] is not None else "-"}</span></div>'
                   f'<div><span class="k">Status</span><span class="v">{badge(hs["status"])}</span></div>'
                   f'<div><span class="k">Last successful run</span><span class="v">{last_ok}</span></div></div>', unsafe_allow_html=True)
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

st.markdown('<div class="bl-foot">BankLake · synthetic data</div>', unsafe_allow_html=True)
