"""Home: the four steps of BankLake as cards with their live status, and one button that continues where the person left off.
Every number is read from the logs and the lake; nothing here is typed in."""
import html

import streamlit as st

from banklake import ingest, meta, pipeline_run
from banklake.transform import catalog, engine

from .. import flow, nav
from ..layout import CTRL
from ..components import badge
from . import etl, report


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
            ready += all((lambda st_: st_[0] == "done" and st_[1])(report.table_state(t, rd_s)) for t in info["tables"])   # loaded and not empty
    val = meta.query("SELECT run_id FROM reconciliation_results ORDER BY id DESC LIMIT 1")
    return dict(run=run[0] if run else None, through=through, rd=rd, done=done, ready=ready, validated=bool(val),
                total={k: sum(1 for p in catalog.PROCS if p["layer"] == k) for k in done}, n_reports=len(report.reports()))


def _card(n, title, state, metrics):
    body = "".join(f"<div class='f'><span>{html.escape(k)}</span><b>{html.escape(v)}</b></div>" for k, v in metrics)
    st.markdown(f"<div class='bl-card'><div class='h'><span class='n'>{n}</span><span class='t'>{html.escape(title)}</span>{badge(state)}</div>{body}</div>",
                unsafe_allow_html=True)


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
    working = pipeline_run.busy()
    with st.container(key="home_bar"):
        a, b = st.columns([sum(CTRL[:2]) + CTRL[2], sum(CTRL[3:])], vertical_alignment="center")
        a.markdown(f"<span class='bl-bar-text'>{html.escape('Run in progress' if working else why)}</span>", unsafe_allow_html=True)
        if b.button(label, width="stretch", key="home_continue", disabled=working):
            nav.open_page(target)

    _run_panel()
    run, d = f["run"], f["done"]
    t = f["total"]
    ing_state = "NOT STARTED" if not run else ("RUNNING" if ingest.running() else run["status"])
    ing_m = [("Loaded through", f"{f['through']:%d %b %Y}" if f["through"] else "-"),
             ("Tables loaded", f"{run['tables_ok']} / {run['tables_total']}" if run else "-")]
    silver_done = d["silver_merge"] + d["silver_transform"]
    silver_all = t["silver_merge"] + t["silver_transform"]
    if f["rd"] is None:
        tr_state = "NOT STARTED"
    else:
        tr_state = "COMPLETED" if d["gold"] == t["gold"] and silver_done == silver_all else "NOT STARTED" if not (silver_done or d["gold"]) else "RUNNING" if engine.running() else "PARTIAL"
    tr_m = [("Silver tables", f"{silver_done} / {silver_all}"), ("Gold tables", f"{d['gold']} / {t['gold']}")]
    rp_state = "NOT READY" if f["ready"] == 0 else "READY"
    rp_m = [("Reports ready", f"{f['ready']} / {f['n_reports']}"), ("Report date", f"{f['rd']:%d %b %Y}" if f["rd"] else "-")]
    v = etl._validation_summary()
    va_state = "NOT STARTED" if not v else v["status"]
    va_m = [("Checks passed", f"{v['passed']} / {v['total']}" if v else "-"), ("Run", v["run_id"] if v else "-")]

    cards = [("1", "Ingestion", ing_state, ing_m, "ingestion", "Open Ingestion"),
             ("2", "Transformation", tr_state, tr_m, "transformation", "Open Transformation"),
             ("3", "Reporting and BI", rp_state, rp_m, "reporting", "Open Reporting"),
             ("4", "Validation", va_state, va_m, "validation", "Open Validation")]
    for col, (n, title, state, metrics, page_key, btn) in zip(st.columns(4), cards):
        with col:
            _card(n, title, state, metrics)
            if st.button(btn, key=f"home_{page_key}", width="stretch"):
                nav.open_page(page_key)


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
    """Overall fraction done across ingestion, Silver and Gold."""
    rows = _steps(run)
    done = sum(r[2] for r in rows)
    total = sum(r[3] for r in rows) or 1
    return done / total


def _chain(run):
    """The path every run takes, with where it is now: Source, Bronze Ingestion, Silver, Gold."""
    names = ["Source", "Bronze Ingestion", "Silver", "Gold"]
    at = {"ingest": 1, "silver": 2, "gold": 3}.get(run["stage"], 4)
    if run["ingest_skipped"]:
        at = max(at, 2)
    parts = []
    for i, n in enumerate(names):
        cls = "done" if i < at else "now" if i == at else "next"
        parts.append(f"<span class='bl-st {cls}'>{n}</span>")
    return "<div class='bl-chain'>" + "<span class='bl-sep'>&rarr;</span>".join(parts) + "</div>"


@st.fragment(run_every=0.5)
def _live():
    run = pipeline_run.current()
    if not run or not pipeline_run.running():
        st.rerun()
        return
    frac = _step_rows(run)
    st.markdown(_chain(run), unsafe_allow_html=True)
    st.progress(min(frac, 1.0), text={"ingest": "Loading source records into Bronze", "silver": "Building Silver", "gold": "Building Gold"}.get(run["stage"], "Working"))


def _result(run, f):
    secs = (run["ended"] - run["started"]) if run.get("ended") else 0
    if run["status"] == "FAILED":
        st.error(run["error"] or "The run failed. Open Transformation to see which table stopped.")
        return
    rd = run["report_date"]
    ready = sum(all((lambda s_: s_[0] == "done" and s_[1])(report.table_state(t, rd)) for t in info["tables"]) for info in report.reports().values())
    line = f"Completed in {secs:,.0f}s · {ready}/{len(report.reports())} reports ready"
    if run["status"] == "SUCCESS":
        st.markdown(f"<div class='bl-done'>{line}</div>", unsafe_allow_html=True)
    else:
        gaps = len(run["failed"]) + len(run["skipped"])
        st.markdown(f"<div class='bl-done warn'>{line} · {gaps} table(s) need attention, see Transformation</div>", unsafe_allow_html=True)


def _run_panel():
    busy = pipeline_run.busy()
    with st.container(border=True, key="home_runner"):
        a, _mid, b = st.columns([CTRL[0], CTRL[1] + CTRL[2], CTRL[3] + CTRL[4]], vertical_alignment="bottom")
        with a:
            fresh = flow.ingested_through() is None
            rd = flow.pick_date("Run date", "home_date", default=flow.next_day_to_ingest() if fresh else None)
        go_ = b.button("Run Entire Pipeline", type="primary", key="home_run_all", disabled=busy, width="stretch")
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
            _result(run, None)
        elif busy:
            st.info("Another run is in progress on the Ingestion or Transformation page.")
