"""Small reusable UI pieces: status badge, metric strip, section heading, note."""
import html

import pandas as pd
import streamlit as st

from .theme import STATUS


def badge(status: str) -> str:
    fg, bg = STATUS.get(str(status).upper(), ("#5E6B7D", "#ECEFF3"))
    return f'<span class="bl-badge" style="color:{fg};background:{bg}">{html.escape(str(status).upper())}</span>'


def strip(items):
    """items: [(label, value, sublabel)] -> one compact bordered row. Values must come from real data."""
    cells = "".join(f'<div><div class="k">{html.escape(str(k))}</div><div class="v">{html.escape(str(v))}</div>'
                    f'<div class="s">{html.escape(str(s))}</div></div>' for k, v, s in items)
    st.markdown(f'<div class="bl-strip">{cells}</div>', unsafe_allow_html=True)


def note(text: str):
    st.markdown(f'<div class="bl-note">{html.escape(text)}</div>', unsafe_allow_html=True)


def fmt_int(x) -> str:
    return "-" if x is None or (isinstance(x, float) and pd.isna(x)) else f"{int(x):,}"


def fmt_dur(sec) -> str:
    if sec is None or (isinstance(sec, float) and pd.isna(sec)):
        return "-"
    sec = float(sec)
    return f"{sec:.1f}s" if sec < 60 else f"{int(sec // 60)}m {int(sec % 60):02d}s"


def table(df: pd.DataFrame, height=None, key=None, **kw):
    """Dense, searchable table. Empty frames say so instead of drawing an empty grid."""
    if df is None or df.empty:
        st.caption("No rows.")
        return
    if height is not None:
        kw["height"] = height
    st.dataframe(df, hide_index=True, width="stretch", key=key, **kw)


def html_table(headers, rows, status_col=None):
    """Compact HTML table (status column rendered as badges). rows: list of tuples; values are escaped."""
    th = "".join(f"<th style='text-align:left;font-size:.68rem;color:#5E6B7D;text-transform:uppercase;letter-spacing:.05em;padding:.3rem .5rem;border-bottom:1px solid #DDE2E9'>{html.escape(h)}</th>" for h in headers)
    body = ""
    for r in rows:
        tds = ""
        for i, v in enumerate(r):
            cell = badge(v) if i == status_col else html.escape(str(v))
            tds += f"<td style='font-size:.8rem;padding:.3rem .5rem;border-bottom:1px solid #EEF1F5;vertical-align:top'>{cell}</td>"
        body += f"<tr>{tds}</tr>"
    st.markdown(f"<table style='width:100%;border-collapse:collapse;border:1px solid #DDE2E9'><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>", unsafe_allow_html=True)
