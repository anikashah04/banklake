"""What the three pages share: one business date that follows the person from Ingestion to Transformation to Reporting,
the last date that really reached Bronze, and the hand-off buttons between pages. Everything is read from the logs and the lake."""
from datetime import date, timedelta

import streamlit as st

from banklake import meta, sources
from banklake.common import layer_path
from banklake.schemas import FIRST_BUSINESS_DATE

from . import nav

KEY = "_bl_date"


def ingested_through():
    """The latest business date that was loaded into Bronze (or was already there), or None before the first ingestion."""
    r = meta.query("SELECT max(business_date) d FROM ingestion_runs WHERE tables_ok > 0 OR tables_skipped > 0")
    return date.fromisoformat(str(r[0]["d"])[:10]) if r and r[0]["d"] else None


def has_bronze():
    return any(any(layer_path("bronze", e).glob("*.parquet")) for e in ("customer", "account"))


def pick_date(label, key, default=None):
    """The shared date picker. The date is remembered across pages and never goes past yesterday (the last closed business day)."""
    latest = sources.latest_business_date()
    first = date.fromisoformat(FIRST_BUSINESS_DATE)
    cur = st.session_state.get(KEY) or default or ingested_through() or latest
    cur = min(max(cur, first), latest)
    d = st.date_input(label, value=cur, min_value=first, max_value=latest, format="DD/MM/YYYY", key=key)
    st.session_state[KEY] = d
    return d


def next_day_to_ingest():
    """Where Ingestion should start: the day after the last loaded one, or the first demo date on a fresh lake."""
    last = ingested_through()
    latest = sources.latest_business_date()
    return min(last + timedelta(days=1), latest) if last else date(2026, 7, 1)


def go(label, page, key, primary=False):
    """A hand-off button to the next or previous page."""
    if st.button(label, key=key, type="primary" if primary else "secondary"):
        nav.open_page(page)


def no_data(next_page_label="Go to Ingestion"):
    st.markdown("#### No data in the lake yet")
    st.write("Start with Ingestion: it loads the source records into Bronze. Then come back here.")
    go(next_page_label, "ingestion", "flow_go_ing", primary=True)


def not_ingested(rd):
    """Warning when the chosen date is later than anything in Bronze. Returns True when the page should stop."""
    through = ingested_through()
    if through is None or rd > through:
        st.warning(f"Bronze only has data up to {through:%d %b %Y}. Ingest the later dates first." if through
                   else "Nothing has been ingested yet.")
        go("Go to Ingestion", "ingestion", "flow_go_ing2")
        return True
    return False
