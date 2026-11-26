"""Home: the four steps of BankLake as cards with their live status, and one button that continues where the person left off.
Every number is read from the logs and the lake; nothing here is typed in."""
import html
from datetime import date

import streamlit as st

from banklake import ingest, meta, pipeline_run
from banklake.transform import catalog, engine

from .. import flow, nav
from ..components import badge
from . import report


def _facts():
    """What has been done so far, as plain numbers."""
    run = meta.query("SELECT business_date, status, tables_ok, tables_failed, tables_total FROM ingestion_runs ORDER BY rowid DESC LIMIT 1")
    through = flow.ingested_through()
    rd = (st.session_state.get(flow.KEY) or through)
    rd_s = rd.isoformat() if rd else None
    done = {"silver_merge": 0, "silver_transform": 0, "gold": 0}
    if rd_s:
        for r in meta.query("SELECT layer, count(DISTINCT target_table_name) n FROM layer_audit_log WHERE report_date=? AND status='SUCCESS' GROUP BY layer", (rd_s,)):
            if r["layer"] in done:
                done[r["layer"]] = r["n"]
    ready = 0
    if rd_s:
        for info in report.reports().values():
            ready += all(report.table_state(t, rd_s)[0] == "done" for t in info["tables"])
    val = meta.query("SELECT run_id FROM reconciliation_results ORDER BY id DESC LIMIT 1")
    return dict(run=run[0] if run else None, through=through, rd=rd, done=done, ready=ready, validated=bool(val),
                total={k: sum(1 for p in catalog.PROCS if p["layer"] == k) for k in done}, n_reports=len(report.reports()))


def _card(n, title, text, state, lines):
    body = "".join(f"<div class='f'>{html.escape(x)}</div>" for x in lines)
    st.markdown(f"<div class='bl-card'><div class='h'><span class='n'>{n}</span><span class='t'>{html.escape(title)}</span>{badge(state)}</div>"
                f"<div class='d'>{html.escape(text)}</div>{body}</div>", unsafe_allow_html=True)


def _next_step(f):
    """The one thing to do next, from what has really happened."""
    if f["through"] is None:
        return "Start with Ingestion", "ingestion", "Nothing is loaded yet. Ingestion brings the source records into Bronze."
    t = f["done"]
    if t["gold"] == 0:
        return "Continue to Transformation", "transformation", f"Bronze holds data up to {f['through']:%d %b %Y}. Next, build Silver and Gold."
    if f["ready"] == 0:
        return "Continue to Transformation", "transformation", "Gold is only partly built. Finish the Gold layer to open the reports."
    return "Open Reporting", "reporting", f"{f['ready']} of {f['n_reports']} reports are ready for {f['rd']:%d %b %Y}."


def page():
    meta.init_db()
    f = _facts()
    label, target, why = _next_step(f)
    a, b = st.columns([3.2, 1.2], vertical_alignment="center")
    working = pipeline_run.busy()
    a.markdown(f"**{html.escape('A run is in progress. This page updates when it finishes.' if working else why)}**")
    if b.button(label, type="primary", width="stretch", key="home_continue", disabled=working):
        nav.open_page(target)

    _run_panel()
    run, d = f["run"], f["done"]
    t = f["total"]
    ing_state = "NOT STARTED" if not run else ("RUNNING" if ingest.running() else run["status"])
    ing_lines = ["No source records loaded yet."] if not run else [
        f"Loaded through {f['through']:%d %b %Y}" if f["through"] else "No tables loaded yet",
        f"Last run: {run['tables_ok']} of {run['tables_total']} tables loaded" + (f", {run['tables_failed']} failed" if run["tables_failed"] else "")]
    silver_done = d["silver_merge"] + d["silver_transform"]
    silver_all = t["silver_merge"] + t["silver_transform"]
    if f["rd"] is None:
        tr_state, tr_lines = "NOT STARTED", ["Needs Bronze data first."]
    else:
        tr_state = "COMPLETED" if d["gold"] == t["gold"] and silver_done == silver_all else "NOT STARTED" if not (silver_done or d["gold"]) else "RUNNING" if engine.running() else "PARTIAL"
        tr_lines = [f"Silver: {silver_done} of {silver_all} tables", f"Gold: {d['gold']} of {t['gold']} tables", f"Report date {f['rd']:%d %b %Y}"]
    rp_state = "NOT READY" if f["ready"] == 0 else "READY"
    rp_lines = [f"{f['ready']} of {f['n_reports']} reports ready"] if f["rd"] else ["Needs Gold tables first."]
    va_state = "COMPLETED" if f["validated"] else "NOT STARTED"
    va_lines = ["Counts compared between the source and every layer." if f["validated"] else "Not run yet."]

    cards = [("1", "Ingestion", "Load source records into Bronze. Personal data is masked here.", ing_state, ing_lines, "ingestion", "Open Ingestion"),
             ("2", "Transformation", "Turn Bronze into Silver and Gold tables.", tr_state, tr_lines, "transformation", "Open Transformation"),
             ("3", "Reporting and BI", "Read Gold as dashboards, reports and BI charts.", rp_state, rp_lines, "reporting", "Open Reporting"),
             ("4", "Validation", "Check that record counts match at every layer.", va_state, va_lines, "validation", "Open Validation")]
    cols = st.columns(4)
    for col, (n, title, text, state, lines, page_key, btn) in zip(cols, cards):
        with col:
            _card(n, title, text, state, lines)
            if st.button(btn, key=f"home_{page_key}", width="stretch"):
                nav.open_page(page_key)
    st.caption("Raw files  →  Bronze  →  Silver Merge  →  Silver Transform  →  Gold  →  Reports and BI")


def _steps(run):
    """(label, state, done, total, note) for Ingestion, Silver and Gold, while or after a full run."""
    out = []
    if run["ingest_skipped"]:
        out.append(("Ingestion", "SKIPPED", 0, 0, "Bronze already holds this date"))
    else:
        snap = ingest.current()
        t = snap["tables"] if snap else []
        fin = sum(x["status"] in ("SUCCESS", "SKIPPED", "FAILED") for x in t)
        st_ = "RUNNING" if run["stage"] == "ingest" else "FAILED" if snap and snap["status"] == "FAILED" else "COMPLETED" if snap and snap["status"] == "SUCCESS" else "PARTIAL"
        out.append(("Ingestion", st_, fin, len(t), f"{fin} of {len(t)} tables"))
    en = engine.current()
    live = en["items"] if en and en["report_date"] == run["report_date"] else []
    groups = {"Silver": run["silver_items"] or (live if run["stage"] == "silver" else []),
              "Gold": [i for i in live if i["layer"] == "gold"] if run["stage"] in ("gold", "done") else []}
    for label, its in groups.items():
        fin = sum(i["status"] in ("SUCCESS", "FAILED", "BLOCKED") for i in its)
        if not its or fin == 0 and not (run["stage"] == label.lower()):
            out.append((label, "NOT STARTED", 0, len(its), "waiting"))
            continue
        bad = any(i["status"] in ("FAILED", "BLOCKED") for i in its)
        st_ = "RUNNING" if fin < len(its) else "PARTIAL" if bad else "COMPLETED"
        out.append((label, st_, fin, len(its), f"{fin} of {len(its)} tables"))
    return out


def _step_rows(run):
    rows = _steps(run)
    done = sum(r[2] for r in rows)
    total = sum(r[3] for r in rows) or 1
    for label, state, d, t, note in rows:
        a, b, c = st.columns([1.2, 1.0, 3.0], vertical_alignment="center")
        a.markdown(f"**{label}**")
        b.markdown(badge(state), unsafe_allow_html=True)
        c.caption(note)
    return done / total


@st.fragment(run_every=0.5)
def _live():
    run = pipeline_run.current()
    if not run or not pipeline_run.running():
        st.rerun()
        return
    frac = _step_rows(run)
    st.progress(min(frac, 1.0), text={"ingest": "Loading source records into Bronze", "silver": "Building Silver", "gold": "Building Gold"}.get(run["stage"], "Working"))


def _result(run):
    mins = (run["ended"] - run["started"]) if run.get("ended") else 0
    d = date.fromisoformat(run["report_date"])
    if run["status"] == "FAILED":
        st.error(run["error"] or "The run failed. Open Transformation to see which table stopped.")
        return
    _step_rows(run)
    if run["status"] == "SUCCESS":
        st.success(f"Pipeline finished for {d:%d %b %Y} in {mins:,.0f} seconds. Silver, Gold and every report are ready.")
    else:
        why = [f"{len(run['failed'])} table(s) failed" if run["failed"] else "", f"{len(run['skipped'])} skipped (no Bronze data)" if run["skipped"] else ""]
        st.warning("Finished with gaps: " + ", ".join(w for w in why if w) + ". Open Transformation for details; the rest is ready.")


def _run_panel():
    busy = pipeline_run.busy()
    with st.container(border=True):
        st.markdown("**Run the entire pipeline for a date**")
        a, b = st.columns([1.2, 3.0], vertical_alignment="bottom")
        with a:
            fresh = flow.ingested_through() is None
            rd = flow.pick_date("Run date", "home_date", default=flow.next_day_to_ingest() if fresh else None)
        through = flow.ingested_through()
        go_ = b.button("Run Entire Pipeline", type="primary", key="home_run_all", disabled=busy,
                       help="Ingestion, then Silver Merge, Silver Transform and Gold for this date")
        st.caption("Loads every source record up to this date into Bronze, builds Silver and Gold, then the reports are ready to open."
                   + (f" Bronze already holds {rd:%d %b %Y}, so ingestion is skipped." if through and rd <= through else ""))
        if go_:
            try:
                pipeline_run.start(rd)
            except ValueError as e:
                st.error(str(e))
                return
            st.rerun()
        run = pipeline_run.current()
        if run and pipeline_run.running():
            _live()
        elif run and run["stage"] == "done":
            _result(run)
        elif busy:
            st.info("Another run is in progress on the Ingestion or Transformation page. Wait for it to finish.")
