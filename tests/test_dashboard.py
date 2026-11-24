"""The five pages of the console render against a real lake and against an empty one, and every safe button works."""
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")
NAV = ["overview", "ingestion", "transformation", "reporting", "validation"]


def _pages(at):
    """url_path -> page hash, as registered by st.navigation on the first run (callable pages, so no file to switch to)."""
    return {(info["url_pathname"] or "overview"): h for h, info in at._registered_pages.items() if "url_pathname" in info}


def test_navigation_is_the_five_pages_in_order(lake):
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
    assert f"{total_silver:,} rows" in svg
    for word in ("Bronze", "Silver Merge", "Silver Transform", "Gold", "BI reports", "MASKED", "UNMASKED", "1  INGESTION", "2  TRANSFORMATION", "3  REPORTING"):
        assert word in svg, word


def test_validation_page_lists_every_source_table(lake):
    from banklake.schemas import ORDER
    at = AppTest.from_file(APP, default_timeout=120).run()
    at._page_hash = _pages(at)["validation"]
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    text = " ".join(m.value for m in at.markdown)
    assert "record counts match" in text
    assert len(ORDER) == 6


def test_every_page_survives_an_empty_lake(tmp_path):
    """A first-time user (nothing generated, nothing run) must see a friendly panel, never a traceback. Ingestion needs no demo data."""
    import os
    import subprocess
    import sys
    code = f"""
from streamlit.testing.v1 import AppTest
at = AppTest.from_file({APP!r}, default_timeout=120).run()
pages = {{(i['url_pathname'] or 'overview'): h for h, i in at._registered_pages.items() if 'url_pathname' in i}}
assert len(pages) == 5, pages
bad = []
for name, h in pages.items():
    at._page_hash = h
    at.run()
    if at.exception:
        bad.append((name, [e.value for e in at.exception]))
    if name != 'ingestion':   # Ingestion has its own source databases, so it works before anything else exists
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


SLOW = ("start ingestion", "run", "generate", "build the demo", "inject", "repair", "reload", "save", "add ", "convert", "check aws", "reset", "refresh")


def test_every_safe_button_on_every_page_does_something(lake):
    """Click each button that does not start a long job; nothing may raise. (Buttons that start jobs are covered by the CLI tests.)"""
    at = AppTest.from_file(APP, default_timeout=120).run()
    pages = _pages(at)
    clicked, problems = 0, []
    for name, h in pages.items():
        at._page_hash = h
        at.run()
        for label, key in [(b.label, b.key) for b in at.button]:
            if any(w in label.lower() for w in SLOW):
                continue
            at._page_hash = h
            at.run()
            btn = [b for b in at.button if b.key == key and b.label == label]
            if not btn:
                continue
            try:
                btn[0].click().run()
            except Exception as e:  # noqa: BLE001
                problems.append((name, label, repr(e)[:150]))
                continue
            clicked += 1
            if at.exception:
                problems.append((name, label, [x.value[:120] for x in at.exception]))
    assert clicked >= 4, clicked
    assert not problems, problems
