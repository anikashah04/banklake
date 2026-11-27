"""BankLake design system: one stylesheet. Navy sidebar, light workspace, muted blue accent, compact type."""
import streamlit as st

NAVY, NAVY_2, ACCENT, INK, MUTED, LINE = "#0F2942", "#1B3A5C", "#0F2942", "#1B2430", "#475569", "#D8E0E8"
GOLD, GREEN, RED, PANEL, BG = "#B8860B", "#1E6B4B", "#A62626", "#EEF2F6", "#F5F7FA"
STATUS = {  # status -> (text colour, background)
    "SUCCESS": ("#1E6B4B", "#E3F1EA"), "OK": ("#1E6B4B", "#E3F1EA"), "AVAILABLE": ("#1E6B4B", "#E3F1EA"), "PASS": ("#1E6B4B", "#E3F1EA"),
    "FAILED": ("#A62626", "#F8E4E2"), "FAIL": ("#A62626", "#F8E4E2"), "HIGH": ("#A62626", "#F8E4E2"),
    "RUNNING": ("#2F5D8A", "#E3ECF5"), "BLOCKED": ("#8A5A12", "#FBF0DC"), "MEDIUM": ("#8A5A12", "#FBF0DC"), "WARNING": ("#8A5A12", "#FBF0DC"),
    "SKIPPED": ("#5E6B7D", "#ECEFF3"), "NOT RUN": ("#5E6B7D", "#ECEFF3"), "LOW": ("#5E6B7D", "#ECEFF3"), "INFO": ("#2F5D8A", "#E3ECF5"),
    "COMPLETED": ("#1E6B4B", "#E3F1EA"), "READY": ("#1E6B4B", "#E3F1EA"), "NOT READY": ("#5E6B7D", "#ECEFF3"), "WARNINGS": ("#8A5A12", "#FBF0DC"), "NOT STARTED": ("#5E6B7D", "#ECEFF3"),
    "PARTIAL": ("#8A5A12", "#FBF0DC"), "NOT CONFIGURED": ("#5E6B7D", "#ECEFF3"), "PLANNED": ("#5E6B7D", "#ECEFF3"), "UNAVAILABLE": ("#5E6B7D", "#ECEFF3"),
    "NOT ENABLED": ("#5E6B7D", "#ECEFF3"), "NO DATA": ("#5E6B7D", "#ECEFF3"),
}

CSS = f"""
<style>
@import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap");
html, body, [class*="css"] {{ font-size: 14px; font-family: Inter, "Source Sans Pro", Arial, sans-serif; }}
.block-container {{ padding-top: 3.2rem; padding-bottom: 2rem; max-width: 100%; }}
header[data-testid="stHeader"] {{ background: transparent; height: 2.2rem; }}
[data-testid="stToolbar"], [data-testid="stDecoration"], footer {{ display: none !important; }}
h1 {{ font-size: 1.35rem !important; font-weight: 650 !important; letter-spacing: -0.01em; padding: 0.2rem 0 0.4rem 0 !important; }}
h2, h3 {{ font-size: 1.02rem !important; font-weight: 650 !important; color: {INK}; margin-top: 0.9rem !important; }}
h4 {{ font-size: 0.92rem !important; font-weight: 650 !important; }}
p, li, label, .stMarkdown {{ font-size: 0.9rem; }}
[data-testid="stCaptionContainer"] {{ color: #4B5563; font-size: .88rem; }}

/* sidebar */
[data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"], [data-testid="collapsedControl"] {{ display: none !important; }}
[data-testid="stSidebar"] {{ background: {NAVY}; border-right: 1px solid #0A1424; min-width: 15rem; }}
[data-testid="stSidebar"] * {{ color: #C7D2E2; }}
[data-testid="stSidebar"] [data-testid="stSidebarHeader"] {{ padding-bottom: 0; }}
[data-testid="stSidebarNavSeparator"] {{ display: none; }}
[data-testid="stSidebarNavLink"] {{ border-radius: 3px; padding: 0.28rem 0.6rem; margin: 1px 0; border-left: 3px solid transparent; }}
[data-testid="stSidebarNavLink"] span {{ font-size: 0.86rem; }}
[data-testid="stSidebarNavLink"]:hover {{ background: {NAVY_2}; }}
[data-testid="stSidebarNavLink"][aria-current="page"] {{ background: {NAVY_2}; border-left: 3px solid #6FA3D6; }}
[data-testid="stSidebarNavLink"][aria-current="page"] span {{ color: #FFFFFF; font-weight: 600; }}
[data-testid="stSidebarNav"] [data-testid="stNavSectionHeader"], [data-testid="stSidebarNav"] header {{ color: #7F8FA6 !important; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.08em; }}
[data-testid="stSidebar"] .stSelectbox label, [data-testid="stSidebar"] .stCaption {{ color: #8FA0B8 !important; font-size: 0.74rem; }}
[data-testid="stSidebar"] [data-baseweb="select"] div, [data-testid="stSidebar"] [data-baseweb="select"] input {{ color:#16233A !important; -webkit-text-fill-color:#16233A !important; opacity:1 !important; }}
[data-testid="stSidebar"] [data-baseweb="select"] svg {{ fill:#E6ECF5 !important; }}
.bl-brand {{ display:flex; align-items:center; gap:.55rem; padding:.2rem .3rem .6rem .3rem; border-bottom:1px solid #1E2F4A; margin-bottom:.4rem; }}
.bl-logo {{ width:26px; height:26px; border-radius:4px; background:#2F5D8A; display:flex; align-items:center; justify-content:center; color:#fff !important; font-weight:700; font-size:.8rem; letter-spacing:.02em; }}
.bl-brand b {{ color:#fff !important; font-size:.98rem; letter-spacing:.14em; }}
.bl-brand small {{ display:block; color:#7F8FA6 !important; font-size:.62rem; letter-spacing:.04em; }}

/* header bar: navy, one row */
.stApp {{ background:{BG}; }}
.st-key-bl_head {{ background:{NAVY}; border-radius:4px; padding:.35rem .8rem; margin-bottom:.6rem; }}
.st-key-bl_head [data-testid="stHorizontalBlock"] {{ align-items:center; }}
.bl-brand2 {{ color:#fff; font-weight:700; letter-spacing:.14em; font-size:.95rem; white-space:nowrap; border-left:3px solid {GOLD}; padding-left:.55rem; }}
.st-key-bl_head [class*="st-key-step_"] {{ margin:0; }}
.st-key-bl_head [class*="st-key-step_"] button {{ background:transparent; border:none; border-bottom:2px solid transparent; border-radius:0; color:#B9C6D6; font-size:.82rem; font-weight:500; min-height:2.1rem; padding:.15rem .3rem; box-shadow:none; }}
.st-key-bl_head [class*="st-key-step_"] button:hover {{ color:#fff; background:transparent; }}
.st-key-bl_head [class*="st-key-step_"] button p {{ white-space:nowrap; }}
.st-key-bl_head [class*="st-key-step_current_"] button {{ color:#fff; font-weight:650; border-bottom:2px solid {GOLD}; cursor:default; }}
.st-key-bl_head [class*="st-key-step_next_"] button {{ color:#E8D9A8; }}
.st-key-bl_head .st-key-bl_refresh button {{ background:transparent; border:1px solid #4A6585; color:#DCE5EF; font-size:.76rem; min-height:1.8rem; padding:.05rem .5rem; }}
.bl-meta {{ display:flex; gap:0; align-items:stretch; background:#fff; border:1px solid {LINE}; border-radius:4px; }}
.bl-meta > div {{ padding:.3rem .8rem; border-right:1px solid {LINE}; }}
.bl-meta > div:last-child {{ border-right:none; }}
.bl-meta .k {{ font-size:.62rem; text-transform:uppercase; letter-spacing:.07em; color:{MUTED}; display:block; line-height:1.1; }}
.bl-meta .v {{ font-size:.8rem; color:{INK}; font-weight:550; }}
.bl-foot {{ text-align:center; color:#64748B; font-size:.72rem; margin-top:1.2rem; }}

/* components */
.bl-badge {{ display:inline-block; font-size:.66rem; font-weight:650; letter-spacing:.04em; padding:.08rem .42rem; border-radius:3px; white-space:nowrap; }}
.bl-strip {{ display:flex; border:1px solid {LINE}; border-radius:4px; background:#fff; margin:.2rem 0 .8rem 0; overflow:hidden; }}
.bl-strip > div {{ flex:1; padding:.5rem .75rem; border-right:1px solid {LINE}; }}
.bl-strip > div:last-child {{ border-right:none; }}
.bl-strip .k {{ font-size:.72rem; text-transform:uppercase; letter-spacing:.06em; color:{MUTED}; }}
.bl-strip .v {{ font-size:1.2rem; font-weight:650; color:{INK}; line-height:1.25; }}
.bl-strip .s {{ font-size:.78rem; color:#4B5563; }}
.bl-node {{ border:1px solid {LINE}; border-top:3px solid #9AA6B6; border-radius:3px; background:#fff; padding:.5rem .6rem; min-height:11.5rem; }}
.bl-node.SUCCESS, .bl-node.AVAILABLE {{ border-top-color:#2E8B62; }}
.bl-node.FAILED {{ border-top-color:#B3392F; }}
.bl-node.RUNNING {{ border-top-color:#2F5D8A; }}
.bl-node.BLOCKED {{ border-top-color:#C48A1D; }}
.bl-node.sel {{ box-shadow: 0 0 0 2px #2F5D8A55; }}
.bl-node .h {{ font-size:.72rem; font-weight:700; letter-spacing:.05em; color:{INK}; text-transform:uppercase; }}
.bl-node .sub {{ font-size:.66rem; color:{MUTED}; margin-bottom:.35rem; }}
.bl-node table {{ width:100%; border-collapse:collapse; margin-top:.3rem; }}
.bl-node td {{ font-size:.72rem; padding:.07rem 0; color:{INK}; }}
.bl-node td:first-child {{ color:{MUTED}; }}
.bl-node td:last-child {{ text-align:right; font-variant-numeric: tabular-nums; }}
.bl-warn {{ font-size:.66rem; color:#8A5A0B; background:#FBF1DC; border-radius:3px; padding:.06rem .35rem; white-space:nowrap; }}
.bl-arrow {{ display:flex; align-items:center; justify-content:center; height:11.5rem; color:#8794A7; font-size:1.2rem; }}
.bl-chips {{ display:flex; gap:.5rem; flex-wrap:wrap; margin-top:.2rem; }}
.bl-chip {{ flex:1; min-width:9rem; border:1px solid {LINE}; border-left:3px solid #9AA6B6; border-radius:3px; padding:.35rem .6rem; background:{PANEL}; }}
.bl-chip .h {{ font-size:.7rem; font-weight:650; color:{INK}; }}
.bl-chip .s {{ font-size:.68rem; color:{MUTED}; }}
.bl-note {{ border-left:3px solid #C48A1D; background:#FBF6EA; padding:.4rem .7rem; font-size:.8rem; color:#5A4513; margin:.3rem 0; }}
div[data-testid="stDataFrame"] {{ border:1px solid {LINE}; border-radius:3px; }}
button[kind="secondary"], button[kind="primary"] {{ border-radius:3px; font-size:.8rem; padding:.15rem .7rem; min-height:2rem; }}
button[kind="primary"] {{ background:{ACCENT}; border-color:{ACCENT}; }}
[data-testid="stTabs"] button {{ font-size:.82rem; }}
[data-testid="stExpander"] {{ border:1px solid {LINE}; border-radius:3px; }}
[data-testid="stExpander"] summary {{ font-size:.82rem; }}
hr {{ margin: .6rem 0; }}

[data-testid="stImage"] button, [data-testid="stImage"] [data-testid="StyledFullScreenButton"] {{ display:none !important; }}
.st-key-tr_card {{ background:#fff; }}
.bl-k {{ font-size:.62rem; text-transform:uppercase; letter-spacing:.07em; color:{MUTED}; }}
.bl-v {{ font-size:.86rem; color:{INK}; overflow-wrap:anywhere; }}
.bl-v.b {{ font-weight:700; font-size:.95rem; }}
.bl-v.num {{ font-variant-numeric:tabular-nums; text-align:right; }}
.bl-err {{ color:{RED}; font-size:.78rem; padding-top:.3rem; border-top:1px solid {PANEL}; }}
/* home */
.st-key-home_bar {{ background:{NAVY}; border-left:4px solid {GOLD}; border-radius:4px; padding:.5rem .9rem; margin-bottom:.7rem; }}
.st-key-home_bar .bl-bar-text {{ color:#fff; font-size:.95rem; font-weight:550; }}
.st-key-home_bar button {{ background:transparent; border:1px solid {GOLD}; color:#F3E3B0; }}
.st-key-home_bar button:hover {{ background:{GOLD}; color:#fff; border-color:{GOLD}; }}
.st-key-home_runner {{ background:#fff; border:1px solid {LINE}; border-radius:4px; }}
.bl-done {{ color:{GREEN}; font-weight:600; font-size:.9rem; padding:.2rem 0; }}
.bl-done.warn {{ color:{GOLD}; }}
/* home cards */
.bl-card {{ border:1px solid {LINE}; border-radius:4px; background:#fff; padding:.8rem .9rem; margin-bottom:.5rem; }}
.bl-card .h {{ display:flex; align-items:center; gap:.5rem; margin-bottom:.35rem; }}
.bl-card .n {{ width:1.4rem; height:1.4rem; border-radius:50%; background:{NAVY}; color:#fff; font-size:.75rem; font-weight:650; display:flex; align-items:center; justify-content:center; }}
.bl-card .t {{ font-weight:650; color:{INK}; flex:1; }}
.bl-card .d {{ color:{MUTED}; font-size:.82rem; margin-bottom:.55rem; }}
.bl-card .f {{ display:flex; justify-content:space-between; font-size:.84rem; color:{MUTED}; padding:.25rem 0; border-top:1px solid #EEF2F6; }}
.bl-card .f b {{ color:{INK}; font-weight:600; font-variant-numeric:tabular-nums; }}
.bl-kpi {{ border:1px solid {LINE}; border-radius:4px; background:#fff; padding:.65rem .85rem .55rem; border-top:3px solid {GOLD}; margin-bottom:.6rem; min-height:6.4rem; }}
.bl-kpi .k {{ font-size:.74rem; text-transform:uppercase; letter-spacing:.06em; color:{MUTED}; }}
.bl-kpi .v {{ font-size:1.65rem; font-weight:650; color:{INK}; line-height:1.25; }}
.bl-kpi .s {{ font-size:.76rem; color:{MUTED}; }}
</style>
"""


def apply():
    st.markdown(CSS, unsafe_allow_html=True)
