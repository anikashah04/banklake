"""Chart library for the Reporting page: 17 Altair chart types plus Lollipop here and the Plotly set in charts_more.py (pie, treemap, sunburst, radar, gauge, five 3D charts), all on one validated categorical palette.
Rules kept here: one y-axis (never dual), categories coloured in a fixed order and never cycled (9th+ folds into "Other"),
thin marks with rounded data ends, a surface-coloured gap between stacked fills, recessive grid, tooltips on every mark."""
import altair as alt

from . import charts_more

# categorical slots 1-8 (light surface), passed through the palette validator: adjacent CVD and normal-vision checks pass
PALETTE = ["#257de0", "#f47230", "#00b894", "#f2a600", "#ee7fac", "#008a17", "#6a4ce2", "#ec4647"]
INK, MUTE, GRID, SURFACE = "#1F2937", "#6B7280", "#E6E9EE", "#FFFFFF"
SEQ = ["#E3EEFB", "#9CC3F0", "#257de0", "#1757A0"]          # one hue, light to dark (magnitude)
H = 280

KINDS = {  # name -> (what it needs from the user)
    "Column": "x category, y measure", "Bar": "x category, y measure", "Stacked column": "x, y, split by", "100% stacked": "x, y, split by",
    "Grouped column": "x, y, split by", "Line": "x, y, optional split", "Area": "x, y", "Stacked area": "x, y, split by",
    "Donut": "slice category, y", "Scatter": "x measure, y measure", "Bubble": "x measure, y measure, size", "Heatmap": "x, y categories, value",
    "Histogram": "one measure", "Box plot": "category, y measure", "Waterfall": "x category, y measure", "Funnel": "x category, y measure",
    "Range": "x category, low, high", "Lollipop": "x category, y measure",
}
KINDS_2D = list(KINDS) + [k for k in charts_more.KINDS_2D if k not in KINDS]
KINDS_3D = list(charts_more.KINDS_3D)
KINDS.update(charts_more.NEEDS)
ALL_KINDS = KINDS_2D + KINDS_3D



def _fold(df, col, keep=8):
    """Keep the top categories by row order and fold the rest into Other, so a 9th hue is never generated."""
    vals = list(dict.fromkeys(df[col].astype(str)))
    if len(vals) <= keep:
        return df
    top = set(vals[:keep - 1])
    out = df.copy()
    out[col] = out[col].astype(str).where(out[col].astype(str).isin(top), "Other")
    return out


def _color(df, col):
    dom = list(dict.fromkeys(df[col].astype(str)))
    return alt.Color(f"{col}:N", scale=alt.Scale(domain=dom, range=PALETTE[:len(dom)]), legend=alt.Legend(orient="bottom", title=None, symbolType="square"))


def _agg(df, dims, measures, how="sum"):
    dims = [d for d in dims if d]
    g = df.groupby(dims, sort=False, dropna=False)[list(measures)]
    out = (g.mean() if how == "mean" else g.max() if how == "max" else g.min() if how == "min" else g.count() if how == "count" else g.sum()).reset_index()
    return out


def _style(ch, title=None, height=H):
    ch = ch.properties(height=height, background=SURFACE, **({"title": alt.TitleParams(title, anchor="start", fontSize=13, fontWeight=600, color=INK)} if title else {}))
    return (ch.configure_axis(gridColor=GRID, domain=False, tickColor=GRID, labelColor=MUTE, titleColor=MUTE, labelFontSize=11, titleFontSize=11)
              .configure_view(stroke=None).configure_legend(labelColor=INK, labelFontSize=11).configure(font="Inter, Segoe UI, Arial, sans-serif"))


def _num(c, fmt=",.0f"):
    return alt.Tooltip(c, format=fmt)


def build(kind, df, x=None, y=None, color=None, size=None, v=None, how="sum", top=None, log=False, title=None, y2=None, height=H, z=None):
    """Return an Altair chart (or a Plotly figure for the kinds in charts_more). y may be a list of measures for Line / Grouped column (they are folded into a split)."""
    if df is None or df.empty:
        return None
    if kind in charts_more.KINDS:
        return charts_more.build(kind, df, x=x, y=y, color=color, size=size, v=v, how=how, top=top, title=title, y2=y2, z=z, height=max(height, 320) if kind.startswith("3D") else height)
    df = df.copy()
    ys = [y] if isinstance(y, str) else list(y or [])
    if kind in ("Grouped column", "Line", "Stacked column", "Column") and len(ys) > 1:
        df = _agg(df, [x], ys, how).melt(id_vars=[x], value_vars=ys, var_name="measure", value_name="value")
        y, color = "value", "measure"
    ysc = alt.Scale(type="log") if log else alt.Undefined
    k = kind
    if k in ("Column", "Bar", "Funnel", "Waterfall", "Donut"):
        d = _agg(df, [x], [y], how)
        d = d.sort_values(y, ascending=False) if k in ("Bar", "Funnel", "Donut") else d
        if top:
            d = d.head(top)
        d[x] = d[x].astype(str)
        if k == "Donut":
            d = _fold(d, x)
            d = d.groupby(x, sort=False)[y].sum().reset_index()
            ch = alt.Chart(d).mark_arc(innerRadius=62, stroke=SURFACE, strokeWidth=2).encode(
                theta=alt.Theta(f"{y}:Q"), color=_color(d, x), tooltip=[x, _num(y, ",.2f")])
            return _style(ch, title, height)
        if k == "Waterfall":
            d["end"] = d[y].cumsum()
            d["start"] = d["end"] - d[y]
            d["dir"] = d[y].apply(lambda n: "Increase" if n >= 0 else "Decrease")
            ch = alt.Chart(d).mark_bar(size=22, cornerRadiusEnd=4, stroke=SURFACE, strokeWidth=2).encode(
                x=alt.X(f"{x}:N", sort=None, title=None, axis=alt.Axis(labelAngle=-30)), y=alt.Y("start:Q", title=None), y2="end:Q",
                color=alt.Color("dir:N", scale=alt.Scale(domain=["Increase", "Decrease"], range=[PALETTE[0], PALETTE[1]]), legend=alt.Legend(orient="bottom", title=None)),
                tooltip=[x, _num(y, ",.2f"), _num("end", ",.2f")])
            return _style(ch, title, height)
        order = None if k == "Column" else "-x"
        if k == "Column":
            ch = alt.Chart(d).mark_bar(size=26, cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=PALETTE[0]).encode(
                x=alt.X(f"{x}:N", sort=None, title=None, axis=alt.Axis(labelAngle=-30)), y=alt.Y(f"{y}:Q", title=None, scale=ysc), tooltip=[x, _num(y, ",.2f")])
        else:
            ch = alt.Chart(d).mark_bar(size=18, cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color=PALETTE[0]).encode(
                y=alt.Y(f"{x}:N", sort=order, title=None), x=alt.X(f"{y}:Q", title=None, scale=ysc), tooltip=[x, _num(y, ",.2f")])
            if k == "Funnel":
                ch = ch.encode(opacity=alt.value(1))
        return _style(ch, title, height if k == "Column" else max(180, 30 * len(d) + 40))
    if k in ("Stacked column", "100% stacked", "Grouped column", "Stacked area", "Line", "Area"):
        dims = [x, color]
        d = _agg(_fold(df, color) if color else df, dims, [y], how).sort_values(x, kind="stable")   # years and months read left to right
        d[x] = d[x].astype(str)
        if color:
            d[color] = d[color].astype(str)
        enc_c = _color(d, color) if color else alt.value(PALETTE[0])
        if k in ("Stacked column", "100% stacked", "Grouped column"):
            stack = "normalize" if k == "100% stacked" else (None if k == "Grouped column" else "zero")
            base = alt.Chart(d).mark_bar(size=22 if k != "Grouped column" else 14, cornerRadiusTopLeft=3, cornerRadiusTopRight=3, stroke=SURFACE, strokeWidth=2)
            enc = dict(x=alt.X(f"{x}:N", sort=None, title=None, axis=alt.Axis(labelAngle=-30)), color=enc_c,
                       y=alt.Y(f"{y}:Q", title=None, stack=stack, scale=ysc, axis=alt.Axis(format="%") if k == "100% stacked" else alt.Axis()),
                       tooltip=[x] + ([color] if color else []) + [_num(y, ",.2f")])
            if k == "Grouped column":
                enc["xOffset"] = f"{color}:N"
            return _style(base.encode(**enc), title, height)
        if k == "Stacked area":
            ch = alt.Chart(d).mark_area(opacity=.9, line={"strokeWidth": 2}, stroke=SURFACE).encode(
                x=alt.X(f"{x}:O", title=None, axis=alt.Axis(labelAngle=-30)), y=alt.Y(f"{y}:Q", title=None, stack="zero"), color=enc_c, tooltip=[x] + ([color] if color else []) + [_num(y, ",.2f")])
            return _style(ch, title, height)
        base = alt.Chart(d).encode(x=alt.X(f"{x}:O", title=None, axis=alt.Axis(labelAngle=-30)), y=alt.Y(f"{y}:Q", title=None, scale=ysc), color=enc_c,
                                    tooltip=[x] + ([color] if color else []) + [_num(y, ",.2f")])
        if k == "Area":
            return _style(alt.Chart(d).mark_area(opacity=.35, line={"strokeWidth": 2, "color": PALETTE[0]}, color=PALETTE[0]).encode(
                x=base.encoding.x, y=base.encoding.y, tooltip=base.encoding.tooltip), title, height)
        return _style(base.mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=64, filled=True, stroke=SURFACE, strokeWidth=2)), title, height)
    if k in ("Scatter", "Bubble"):
        d = _agg(_fold(df, color) if color else df, [color] if color else [x], list(dict.fromkeys(m for m in (x, y, size) if m and m != color)), how) if color else df
        enc = dict(x=alt.X(f"{x}:Q", title=x.replace("_", " "), scale=ysc), y=alt.Y(f"{y}:Q", title=y.replace("_", " ")),
                   color=_color(d, color) if color else alt.value(PALETTE[0]), tooltip=([color] if color else []) + [_num(m, ",.2f") for m in (x, y, size) if m])
        if k == "Bubble" and size:
            enc["size"] = alt.Size(f"{size}:Q", legend=None, scale=alt.Scale(range=[120, 1400]))
        return _style(alt.Chart(d).mark_circle(opacity=.85, stroke=SURFACE, strokeWidth=2, **({} if k == "Bubble" else {"size": 110})).encode(**enc), title, height)
    if k == "Heatmap":
        d = _agg(df, [x, y], [v], how)
        d[x], d[y] = d[x].astype(str), d[y].astype(str)
        base = alt.Chart(d).encode(x=alt.X(f"{x}:N", title=None, sort=None, axis=alt.Axis(labelAngle=-30)), y=alt.Y(f"{y}:N", title=None, sort=None))
        rect = base.mark_rect(stroke=SURFACE, strokeWidth=2, cornerRadius=3).encode(
            color=alt.Color(f"{v}:Q", scale=alt.Scale(range=SEQ), legend=alt.Legend(orient="bottom", title=None, gradientLength=160, format=",.0f")), tooltip=[x, y, _num(v, ",.2f")])
        return _style(rect, title, max(180, 34 * d[y].nunique() + 60))
    if k == "Histogram":
        ch = alt.Chart(df).mark_bar(color=PALETTE[0], cornerRadiusTopLeft=3, cornerRadiusTopRight=3, stroke=SURFACE, strokeWidth=2).encode(
            x=alt.X(f"{x}:Q", bin=alt.Bin(maxbins=12), title=x.replace("_", " ")), y=alt.Y("count()", title="rows"), tooltip=[alt.Tooltip("count()", title="rows")])
        return _style(ch, title, height)
    if k == "Box plot":
        d = df.copy()
        d[x] = d[x].astype(str)
        ch = alt.Chart(d).mark_boxplot(size=26, color=PALETTE[0], median={"color": INK}, extent="min-max").encode(
            x=alt.X(f"{x}:N", title=None, axis=alt.Axis(labelAngle=-30)), y=alt.Y(f"{y}:Q", title=None, scale=ysc))
        return _style(ch, title, height)
    if k == "Range":
        d = _agg(df, [x], [y, y2], "min" if how == "sum" else how)
        d[x] = d[x].astype(str)
        base = alt.Chart(d).encode(y=alt.Y(f"{x}:N", title=None, sort=None))
        rule = base.mark_rule(strokeWidth=3, color=PALETTE[0]).encode(x=alt.X(f"{y}:Q", title=None, scale=alt.Scale(type="log") if log else alt.Undefined), x2=f"{y2}:Q")
        lo = base.mark_circle(size=90, color=PALETTE[0], stroke=SURFACE, strokeWidth=2).encode(x=f"{y}:Q", tooltip=[x, _num(y, ",.4f"), _num(y2, ",.4f")])
        hi = base.mark_circle(size=90, color=PALETTE[1], stroke=SURFACE, strokeWidth=2).encode(x=f"{y2}:Q", tooltip=[x, _num(y, ",.4f"), _num(y2, ",.4f")])
        return _style(rule + lo + hi, title, max(180, 30 * len(d) + 50))
    if k == "Lollipop":
        d = _agg(df, [x], [y], how).sort_values(y, ascending=False)
        if top:
            d = d.head(top)
        d[x] = d[x].astype(str)
        base = alt.Chart(d).encode(y=alt.Y(f"{x}:N", sort=None, title=None), x=alt.X(f"{y}:Q", title=None, scale=ysc), tooltip=[x, _num(y, ",.2f")])
        return _style(base.mark_rule(strokeWidth=3, color=PALETTE[0]).encode(x2=alt.value(0)) + base.mark_circle(size=130, color=PALETTE[0], stroke=SURFACE, strokeWidth=2),
                      title, max(180, 30 * len(d) + 50))
    raise ValueError(f"Unknown chart type: {kind}")


def render(ch, key):
    """Draw an Altair chart or a Plotly figure, whichever build() returned."""
    import streamlit as st
    if isinstance(ch, alt.TopLevelMixin):
        st.altair_chart(ch, width="stretch", key=key)
    else:
        st.plotly_chart(ch, width="stretch", key=key, config={"displaylogo": False, "responsive": True})
