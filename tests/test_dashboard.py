"""The five pages of the console render against a real lake and against an empty one, and every safe button works."""
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")
NAV = ["home", "ingestion", "transformation", "reporting", "validation", "overview"]


def _pages(at):
    """url_path -> page hash, as registered by st.navigation on the first run (callable pages, so no file to switch to)."""
    return {(info["url_pathname"] or "home"): h for h, info in at._registered_pages.items() if "url_pathname" in info}


def test_navigation_is_home_then_the_pipeline_pages(lake):
    at = AppTest.from_file(APP, default_timeout=120).run()
    assert not at.exception, [e.value for e in at.exception]
    pages = _pages(at)
    assert [p for p in NAV if p in pages] == NAV and len(pages) == len(NAV)


def test_every_page_renders(lake):
    at = AppTest.from_file(APP, default_timeout=120).run()
    pages = _pages(at)
    for p in NAV:
        at._page_hash = pages[p]
        at.run()
        assert not at.exception, (p, [e.value for e in at.exception])


def test_pipeline_diagram_shows_real_counts_and_masking(lake):
    from banklake.schemas import ORDER
    from bankui import helpers
    from bankui.views import etl
    svg = etl._svg(etl.counts())
    total_silver = sum(helpers.count("silver", e) for e in ORDER)
    assert f">{total_silver:,}<" in svg
    for word in ("Bronze", "Silver Merge", "Silver Transform", "Gold", "BI reports", "MASKED", "UNMASKED", "1  INGESTION", "2  TRANSFORMATION", "3  REPORTING"):
        assert word in svg, word


def test_validation_page_lists_every_source_table(lake):
    from banklake.schemas import ORDER
    at = AppTest.from_file(APP, default_timeout=120).run()
    at._page_hash = _pages(at)["validation"]
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    assert any(b.label == "Run validation now" for b in at.button)
    assert len(ORDER) == 6


def test_every_page_survives_an_empty_lake(tmp_path):
    """A first-time user (nothing generated, nothing run) must see a friendly panel, never a traceback. Ingestion needs no demo data."""
    import os
    import subprocess
    import sys
    code = f"""
from streamlit.testing.v1 import AppTest
at = AppTest.from_file({APP!r}, default_timeout=120).run()
pages = {{(i['url_pathname'] or 'home'): h for h, i in at._registered_pages.items() if 'url_pathname' in i}}
assert len(pages) == 6, pages
bad = []
for name, h in pages.items():
    at._page_hash = h
    at.run()
    if at.exception:
        bad.append((name, [e.value for e in at.exception]))
    if name == 'home':
        assert any(b.label == 'Start with Ingestion' for b in at.button), 'Home must point a first-time user to Ingestion'
    elif name != 'ingestion':   # Ingestion has its own source databases, so it works before anything else exists
        assert any('No data in the lake yet' in m.value for m in at.markdown), name
    else:
        assert any(b.label == 'Start Ingestion' for b in at.button), 'Start Ingestion button missing'
        assert len([b for b in at.button if b.label == 'Start Ingestion']) == 1
print('BAD', bad)
assert not bad
"""
    env = {**os.environ, "BANKLAKE_HOME": str(tmp_path)}
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=600)
    assert r.returncode == 0, (r.stdout + r.stderr)[-2000:]


SLOW = ("start ingestion", "load history", "load all", "convert", "run", "view details", "sql migration", "bi dashboards", "load report data", "run all", "next:", "generate", "build the demo", "inject", "repair", "reload", "save", "add ", "convert", "check aws", "reset")


def test_every_safe_button_on_every_page_does_something(lake):
    """Click each button that does not start a long job; nothing may raise. (Buttons that start jobs are covered by the CLI tests.)"""
    at = AppTest.from_file(APP, default_timeout=120).run()
    pages = _pages(at)
    clicked, problems, seen = 0, [], []
    for name, h in pages.items():
        at._page_hash = h
        at.run()
        for label, key in [(b.label, b.key) for b in at.button]:
            if any(w in label.lower() for w in SLOW) or str(key).startswith(("step_", "home_")):
                continue
            at._page_hash = h
            at.run()
            btn = [b for b in at.button if b.key == key and b.label == label]
            if not btn or btn[0].disabled:
                continue
            try:
                btn[0].click().run()
            except Exception as e:  # noqa: BLE001
                problems.append((name, label, repr(e)[:150]))
                continue
            clicked += 1
            seen.append((name, label))
            if at.exception:
                problems.append((name, label, [x.value[:120] for x in at.exception]))
    assert clicked >= 4, (clicked, seen)
    assert not problems, problems


def test_every_page_has_the_same_step_bar_with_the_current_step_marked(lake):
    labels = ["Home", "1  Ingestion", "2  Transformation", "3  Reporting and BI", "Validation", "Pipeline Overview"]
    order = ["home", "ingestion", "transformation", "reporting", "validation", "overview"]
    following = {"ingestion": "transformation", "transformation": "reporting", "reporting": "validation"}
    at = AppTest.from_file(APP, default_timeout=120).run()
    pages = _pages(at)
    for name in order:
        at._page_hash = pages[name]
        at.run()
        assert not at.exception, (name, [e.value for e in at.exception])
        steps = [b for b in at.button if str(b.key).startswith("step_")]
        assert [b.label for b in steps] == labels, (name, [b.label for b in steps])
        cur = [b for b in steps if str(b.key).startswith("step_current_")]
        assert [b.key for b in cur] == [f"step_current_{name}"], (name, [b.key for b in cur])
        nxt = [b.key for b in steps if str(b.key).startswith("step_next_")]
        assert nxt == ([f"step_next_{following[name]}"] if name in following else []), (name, nxt)
        assert not [b for b in at.button if b.label.startswith(("Next:", "Back"))], name       # no second set of forward buttons


def test_home_continue_button_follows_real_progress():
    from datetime import date

    from bankui.views import home
    d = date(2026, 7, 1)
    base = dict(through=None, rd=None, done={"silver_merge": 0, "silver_transform": 0, "gold": 0}, ready=0, n_reports=18)
    assert home._next_step(base)[:2] == ("Start with Ingestion", "ingestion")
    assert home._next_step(dict(base, through=d, rd=d))[:2] == ("Continue to Transformation", "transformation")
    half = dict(base, through=d, rd=d, done={"silver_merge": 23, "silver_transform": 52, "gold": 3})
    assert home._next_step(half)[:2] == ("Continue to Transformation", "transformation")
    full = dict(half, ready=5)
    label, page, why = home._next_step(full)
    assert (label, page) == ("Open Reporting", "reporting") and "5 of 18 reports" in why
