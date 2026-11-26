"""Reporting page: report kinds, chart library, every report built from real loaded Gold tables."""
import datetime

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from banklake import meta
from banklake.transform import catalog, engine
from dashboard.bankui.views import charts, report
from dashboard.bankui.views.report_specs import SPECS, TYPE_NOTE

RD = "2026-11-25"
ORDER = "ORD-TEST-REPORT"


def test_every_report_has_a_spec_and_a_kind_with_its_own_layout():
    meta.init_db()
    cat = report.reports()
    assert set(cat) == set(SPECS) and {v["type"] for v in cat.values()} == {"Dashboard", "Report", "Regulatory", "BI"} == set(TYPE_NOTE)
    assert set(report.LAYOUT) == set(TYPE_NOTE)
    for n, v in cat.items():                                       # a spec only reads tables the report is built from
        sp = SPECS[n]
        used = {k[1] for k in sp.get("kpis", [])} | {p["table"] for p in sp.get("panels", [])} | set(sp.get("tables", []))
        assert used <= set(v["tables"]), n


def test_chart_library_has_seventeen_types_and_each_compiles():
    df = pd.DataFrame({"cat": list("ABCABC"), "grp": ["u", "u", "u", "v", "v", "v"], "m": [3, 5, 2, 4, 1, 6], "n": [1.5, 2, 3, 4, 5, 6]})
    spec = {"Column": dict(x="cat", y="m"), "Bar": dict(x="cat", y="m"), "Stacked column": dict(x="cat", y="m", color="grp"), "100% stacked": dict(x="cat", y="m", color="grp"),
            "Grouped column": dict(x="cat", y=["m", "n"]), "Line": dict(x="cat", y="m", color="grp"), "Area": dict(x="cat", y="m"),
            "Stacked area": dict(x="cat", y="m", color="grp"), "Donut": dict(x="cat", y="m"), "Scatter": dict(x="m", y="n", color="cat"),
            "Bubble": dict(x="m", y="n", size="n", color="cat"), "Heatmap": dict(x="cat", y="grp", v="m"), "Histogram": dict(x="m"),
            "Box plot": dict(x="grp", y="m"), "Waterfall": dict(x="cat", y="m"), "Funnel": dict(x="cat", y="m"), "Range": dict(x="cat", y="m", y2="n")}
    assert len(charts.KINDS) == 17 and set(spec) == set(charts.KINDS)
    for k, kw in spec.items():
        assert charts.build(k, df, **kw).to_dict()
    assert charts.build("Column", df.iloc[0:0], x="cat", y="m") is None


def test_ninth_category_folds_into_other_instead_of_a_new_colour():
    df = pd.DataFrame({"x": ["a"] * 12, "g": [f"g{i}" for i in range(12)], "v": range(12)})
    d = charts.build("Stacked column", df, x="x", y="v", color="g").to_dict()
    dom = d["encoding"]["color"]["scale"]["domain"]
    assert len(dom) == 8 and dom[-1] == "Other" and len(d["encoding"]["color"]["scale"]["range"]) == 8


def test_helpers():
    assert report.money(12_500_000) == "₹1.25 Cr" and report.money(250_000) == "₹2.50 L" and report.money(900) == "₹900"
    df = pd.DataFrame({"customer_type": ["A", "B"], "total_customers": [3, 4], "avg_score": [1.5, 2.5], "maturity_year": [2027, 2028], "customer_id": ["1", "2"]})
    d, m = report.dims_and_measures(df)
    assert d == ["customer_type", "maturity_year"] and m == ["total_customers", "avg_score"]
    assert report.value(df, "total_customers", "sum", "int") == "7" and report.value(df, "avg_score", "mean", "dec") == "2.00"


def test_table_state_reflects_real_loads(loaded):
    assert report.table_state("gold_customer_overview", RD)[0] == "done"
    assert report.table_state("gold_customer_overview", RD)[1] == len(report.frame("gold_customer_overview", RD))
    assert report.table_state("gold_customer_overview", "2026-01-05") == ("waiting", None)


@pytest.mark.parametrize("name", sorted(SPECS))
def test_every_report_builds_from_real_gold_data(loaded, name):
    """Every column a spec names exists in the loaded table and every chart compiles with data in it."""
    cat = report.reports()
    frames = {t: report.frame(t, RD) for t in cat[name]["tables"]}
    assert all(not f.empty for f in frames.values()), name
    sp = SPECS[name]
    for lab, t, c, how, fmt in sp.get("kpis", []):
        assert c in frames[t].columns, (name, c)
        assert report.value(frames[t], c, how, fmt)
    for p in sp.get("panels", []):
        f = frames[p["table"]]
        kw = {k: v for k, v in p.items() if k not in ("table", "chart", "title")}
        for k in ("x", "y", "color", "size", "v", "y2"):
            for c in ([kw[k]] if isinstance(kw.get(k), str) else kw.get(k, [])):
                assert c in f.columns, (name, p["title"], c)
        assert charts.build(p["chart"], f, **kw).to_dict()
    if "table" in sp:
        assert set(sp["table"]["cols"]) <= set(frames[sp["table"]["table"]].columns)
    if "share" in sp:
        assert {sp["share"]["by"], sp["share"]["measure"]} <= set(frames[sp["share"]["table"]].columns)
    for t in sp.get("tables", []):                                 # BI: every chart type works on every table it can open
        d, m = report.dims_and_measures(frames[t])
        assert d and m, t


def _app():
    from dashboard.bankui.views import report
    report.page()


@pytest.mark.parametrize("name", sorted(SPECS))
def test_page_renders_each_report_without_error(loaded, name):
    at = AppTest.from_function(_app, default_timeout=180).run()
    at.date_input[0].set_value(datetime.date(2026, 11, 25)).run()
    at.selectbox[0].select_index(sorted(report.reports()).index(name) if False else list(report.reports()).index(name)).run()
    assert not at.exception, (name, at.exception)
    text = " ".join(m.value for m in at.markdown)
    assert "READY" in text and "NOT READY" not in text
    kind = report.reports()[name]["type"]
    if kind == "Dashboard":
        assert len(at.metric) >= 2
    if kind == "Regulatory":
        assert "PASS" in text
    if kind == "BI":
        assert any(s.label == "Chart type" for s in at.selectbox)


def test_bi_explorer_switches_chart_types(loaded):
    at = AppTest.from_function(_app, default_timeout=180).run()
    at.date_input[0].set_value(datetime.date(2026, 11, 25)).run()
    at.selectbox[0].select("Monthly Transaction Trends").run()
    ck = [s for s in at.selectbox if s.label == "Chart type"][0]
    for kind in charts.KINDS:
        ck.select(kind).run()
        assert not at.exception, (kind, at.exception)
        ck = [s for s in at.selectbox if s.label == "Chart type"][0]


def test_bi_dashboards_button_opens_its_dialog(loaded):
    """Regression: the dialog function was shadowed by the BI explorer function of the same name."""
    at = AppTest.from_function(_app, default_timeout=180).run()
    [b for b in at.button if b.label == "BI Dashboards"][0].click().run()
    assert not at.exception, at.exception
    assert callable(report._bi_dashboards) and report._bi is not report._bi_dashboards


def _transform_app():
    from dashboard.bankui.views import transform
    transform.page()


@pytest.mark.parametrize("fn,label", [(_app, "View Details"), (_transform_app, "View Details"), (_transform_app, "SQL Migration Tool")])
def test_dialog_buttons_open_without_error(loaded, fn, label):
    at = AppTest.from_function(fn, default_timeout=180).run()
    [b for b in at.button if b.label == label][0].click().run()
    assert not at.exception, (label, at.exception)
