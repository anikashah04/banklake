"""Extra tool: Teradata stored procedure -> Athena SQL + an EMR (PySpark) script."""
from pathlib import Path

import streamlit as st

from banklake.sqlmigrate import procs

from ..components import badge, note

EX = Path(__file__).resolve().parents[3] / "sql_examples" / "proc_refresh_customer_summary.sql"


def render():
    st.markdown("#### Convert a Teradata procedure")
    st.caption("Paste a Teradata stored procedure. Each SQL statement is converted to Athena SQL, and an EMR (PySpark) script is generated from them. "
               "Logic such as DECLARE, SET, IF and loops is listed for a person to rewrite; it is never guessed.")
    if "proc_text" not in st.session_state:
        st.session_state["proc_text"] = EX.read_text() if EX.exists() else ""
    c1, c2 = st.columns([5, 1], vertical_alignment="bottom")
    c2.button("Load example", on_click=lambda: st.session_state.update(proc_text=EX.read_text()), width="stretch")
    text = st.text_area("Teradata procedure", key="proc_text", height=300)
    if not text.strip():
        return
    res = procs.convert_procedure(text)
    n = res.counts
    st.markdown(f"**{res.name}**: {len(res.statements)} steps, {n['CONVERTED']} converted, {n['MANUAL']} need a manual rewrite, {n['UNSUPPORTED']} unsupported.")
    t1, t2, t3 = st.tabs(["Athena SQL", "EMR script", "Step by step"])
    with t1:
        ok = [s for s in res.statements if s.status == "CONVERTED"]
        st.code("\n\n".join(s.converted for s in ok) or "-- nothing converted", language="sql")
        st.download_button("Download Athena SQL", "\n\n".join(s.converted for s in ok), f"{res.name}_athena.sql")
    with t2:
        good, err = procs.script_compiles(res.script)
        (st.success if good else st.error)("Script is valid Python." if good else f"Script has a syntax error: {err}")
        st.code(res.script, language="python")
        st.download_button("Download EMR script", res.script, f"{res.name}_emr.py")
        st.caption("Generated and syntax-checked here. It has not been run on EMR; EMR Serverless is not connected in this project yet.")
    with t3:
        for s in res.statements:
            with st.expander(f"Step {s.n}: {s.status.title()}", expanded=s.status != "CONVERTED"):
                a, b = st.columns(2)
                a.markdown("**Teradata**"), a.code(s.legacy, language="sql")
                b.markdown("**Athena**" if s.status == "CONVERTED" else "**Result**")
                b.code(s.converted if s.status == "CONVERTED" else "-- needs a person", language="sql")
                for x in s.notes:
                    st.caption("• " + x)
