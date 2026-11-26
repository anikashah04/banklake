"""The charts Altair cannot draw: pie, treemap, sunburst, radar, gauge and five 3D charts, built with Plotly on the same palette.
Same rules as charts.py: categories coloured in a fixed order (9th+ folds into Other), one hue light-to-dark for magnitude, quiet grid, hover on every mark."""
import numpy as np
import pandas as pd
import plotly.graph_objects as go

PALETTE = ["#257de0", "#f47230", "#00b894", "#f2a600", "#ee7fac", "#008a17", "#6a4ce2", "#ec4647"]
SEQ = [[0, "#E3EEFB"], [0.4, "#9CC3F0"], [0.75, "#257de0"], [1, "#1757A0"]]
INK, MUTE, GRID = "#1F2937", "#6B7280", "#E6E9EE"
FONT = dict(family="Inter, Segoe UI, Arial, sans-serif", size=12, color=INK)
KINDS_2D = ["Pie", "Treemap", "Sunburst", "Radar", "Gauge", "Lollipop"]
KINDS_3D = ["3D Scatter", "3D Bubble", "3D Line", "3D Column", "3D Surface"]
KINDS = KINDS_2D[:-1] + KINDS_3D            # Lollipop is drawn by Altair
NEEDS = {"Pie": "slice category, y", "Treemap": "category, y, optional group", "Sunburst": "category, y, optional group", "Radar": "category, y, optional split",
         "Gauge": "measure, out of", "Lollipop": "x category, y measure", "3D Scatter": "x, y, z measures", "3D Bubble": "x, y, z, size",
         "3D Line": "x, series, z measure", "3D Column": "x, y categories, z measure", "3D Surface": "x, y categories, z measure"}


def _agg(df, dims, measures, how="sum"):
    dims = [d for d in dims if d]
    g = df.groupby(dims, sort=False, dropna=False)[list(measures)]
    return (g.mean() if how == "mean" else g.max() if how == "max" else g.min() if how == "min" else g.count() if how == "count" else g.sum()).reset_index()


def _fold(d, col, val, keep=8):
    """Biggest 7 categories keep their own colour, the rest become Other."""
    d = d.sort_values(val, ascending=False)
    if len(d) <= keep:
        return d
    top, rest = d.head(keep - 1), d.iloc[keep - 1:]
    return pd.concat([top, pd.DataFrame({col: ["Other"], val: [rest[val].sum()]})], ignore_index=True)


def _layout(fig, title=None, height=300, legend=True):
    fig.update_layout(title=dict(text=title, x=0, font=dict(size=13, color=INK)) if title else None, height=height, font=FONT,
                      paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF", margin=dict(l=10, r=10, t=36 if title else 8, b=8), showlegend=legend,
                      legend=dict(orientation="h", y=-0.08, x=0, title=None, font=dict(size=11)))
    return fig


def _scene(fig, xt, yt, zt, height):
    ax = dict(gridcolor=GRID, backgroundcolor="#FFFFFF", showbackground=True, zerolinecolor=GRID, color=MUTE)
    fig.update_layout(height=height, scene=dict(xaxis=dict(title=xt, **ax), yaxis=dict(title=yt, **ax), zaxis=dict(title=zt, **ax), camera=dict(eye=dict(x=1.6, y=1.6, z=0.9))))
    return fig


def _short(s):
    return str(s).replace("_", " ")


def _box(cx, cy, w, h):
    """Eight corners and twelve triangles of one 3D column standing on z=0."""
    x0, x1, y0, y1 = cx - w, cx + w, cy - w, cy + w
    X = [x0, x1, x1, x0, x0, x1, x1, x0]
    Y = [y0, y0, y1, y1, y0, y0, y1, y1]
    Z = [0, 0, 0, 0, h, h, h, h]
    I = [0, 0, 4, 4, 0, 0, 1, 1, 2, 2, 3, 3]
    J = [1, 2, 5, 6, 1, 5, 2, 6, 3, 7, 0, 4]
    K = [2, 3, 6, 7, 5, 4, 6, 5, 7, 6, 4, 7]
    return X, Y, Z, I, J, K


def build(kind, df, x=None, y=None, color=None, size=None, v=None, how="sum", top=None, title=None, y2=None, z=None, height=300, **_):
    if df is None or df.empty:
        return None
    df = df.copy()
    if kind == "Pie":
        d = _agg(df, [x], [y], how)
        d[x] = d[x].astype(str)
        d = _fold(d, x, y)
        fig = go.Figure(go.Pie(labels=d[x], values=d[y], hole=0, sort=False, marker=dict(colors=PALETTE[:len(d)], line=dict(color="#FFFFFF", width=2)),
                               textinfo="percent", hovertemplate="%{label}<br>%{value:,.2f} (%{percent})<extra></extra>"))
        return _layout(fig, title, height)
    if kind in ("Treemap", "Sunburst"):
        group = color if color and color != x else None
        d = _agg(df, [group, x] if group else [x], [y], how)
        d[x] = d[x].astype(str)
        if not group:
            d = _fold(d, x, y)
        labels, parents, values, ids, cols = [], [], [], [], []
        if group:
            d[group] = d[group].astype(str)
            for i, (g, s) in enumerate(d.groupby(group, sort=False)[y].sum().items()):
                labels.append(g); parents.append(""); values.append(s); ids.append(f"g:{g}"); cols.append(PALETTE[min(i, 7)])
            gcol = {g: PALETTE[min(i, 7)] for i, g in enumerate(d[group].drop_duplicates())}
            for _, r in d.iterrows():
                labels.append(r[x]); parents.append(f"g:{r[group]}"); values.append(r[y]); ids.append(f"g:{r[group]}|{r[x]}"); cols.append(gcol[r[group]])
        else:
            for i, (_, r) in enumerate(d.iterrows()):
                labels.append(r[x]); parents.append(""); values.append(r[y]); ids.append(f"c:{r[x]}"); cols.append(PALETTE[min(i, 7)])
        cls = go.Treemap if kind == "Treemap" else go.Sunburst
        extra = dict(tiling=dict(pad=3)) if kind == "Treemap" else dict(insidetextorientation="radial")
        fig = go.Figure(cls(labels=labels, parents=parents, values=values, ids=ids, branchvalues="total", marker=dict(colors=cols, line=dict(color="#FFFFFF", width=2)),
                            hovertemplate="%{label}<br>%{value:,.2f}<extra></extra>", **extra))
        return _layout(fig, title, height, legend=False)
    if kind == "Radar":
        d = _agg(df, [x, color] if color and color != x else [x], [y], how)
        d[x] = d[x].astype(str)
        fig = go.Figure()
        series = [(None, d)] if not (color and color != x) else [(str(k), g) for k, g in d.groupby(color, sort=False)][:8]
        for i, (name, g) in enumerate(series):
            cats = list(g[x]) + [g[x].iloc[0]]
            vals = list(g[y]) + [g[y].iloc[0]]
            fig.add_trace(go.Scatterpolar(r=vals, theta=cats, name=name or _short(y), fill="toself", opacity=.55, line=dict(color=PALETTE[i], width=2),
                                          hovertemplate="%{theta}<br>%{r:,.2f}<extra></extra>"))
        fig.update_layout(polar=dict(radialaxis=dict(gridcolor=GRID, color=MUTE), angularaxis=dict(gridcolor=GRID)))
        return _layout(fig, title, height, legend=bool(color and color != x))
    if kind == "Gauge":
        val = float(_agg(df.assign(_k=0), ["_k"], [y], how)[y].iloc[0])
        out = float(_agg(df.assign(_k=0), ["_k"], [y2], how)[y2].iloc[0]) if y2 else val * 1.25
        out = out if out else max(val, 1)
        fig = go.Figure(go.Indicator(mode="gauge+number", value=val, number=dict(valueformat=",.2f", font=dict(size=30)),
                                     gauge=dict(axis=dict(range=[0, max(out, val)], tickcolor=MUTE), bar=dict(color=PALETTE[0], thickness=.6), bgcolor="#F1F4F8", borderwidth=0,
                                                threshold=dict(line=dict(color=PALETTE[1], width=3), thickness=.8, value=out))))
        fig.add_annotation(text=f"{_short(y)} out of {_short(y2) if y2 else 'target'} ({val / out:.0%})", x=.5, y=-0.05, showarrow=False, font=dict(color=MUTE, size=12))
        return _layout(fig, title, height, legend=False)
    if kind == "3D Scatter" or kind == "3D Bubble":
        keep = list(dict.fromkeys(m for m in (x, y, z, size) if m))
        d = _agg(df, [color], keep, how) if color and color not in keep else df
        if color and color in keep:
            color = None
        fig = go.Figure()
        groups = [(str(k), g) for k, g in d.groupby(color, sort=False)][:8] if color else [(None, d)]
        for i, (name, g) in enumerate(groups):
            sz = 8
            if kind == "3D Bubble" and size:
                s = g[size].astype(float)
                sz = 6 + 26 * (s - d[size].min()) / ((d[size].max() - d[size].min()) or 1)
            fig.add_trace(go.Scatter3d(x=g[x], y=g[y], z=g[z], mode="markers", name=name or _short(z), showlegend=bool(color),
                                       marker=dict(size=sz, color=PALETTE[i], opacity=.85, line=dict(color="#FFFFFF", width=1)),
                                       hovertemplate=f"{_short(x)} %{{x:,.2f}}<br>{_short(y)} %{{y:,.2f}}<br>{_short(z)} %{{z:,.2f}}<extra>{name or ''}</extra>"))
        return _scene(_layout(fig, title, height, legend=bool(color)), _short(x), _short(y), _short(z), height)
    if kind == "3D Line":
        d = _agg(df, [x, color] if color and color != x else [x], [z], how)
        d[x] = d[x].astype(str)
        fig = go.Figure()
        groups = [(str(k), g) for k, g in d.groupby(color, sort=False)][:8] if color and color != x else [(_short(z), d)]
        for i, (name, g) in enumerate(groups):
            fig.add_trace(go.Scatter3d(x=g[x], y=[name] * len(g), z=g[z], mode="lines+markers", name=name, line=dict(color=PALETTE[i], width=5),
                                       marker=dict(size=4, color=PALETTE[i]), hovertemplate=f"%{{x}}<br>{name}<br>%{{z:,.2f}}<extra></extra>"))
        return _scene(_layout(fig, title, height, legend=False), _short(x), _short(color) if color else "series", _short(z), height)
    if kind in ("3D Column", "3D Surface"):
        d = _agg(df, [x, y], [z], how)
        d[x], d[y] = d[x].astype(str), d[y].astype(str)
        xs, ys = list(dict.fromkeys(d[x])), list(dict.fromkeys(d[y]))
        M = d.pivot_table(index=y, columns=x, values=z, aggfunc="sum").reindex(index=ys, columns=xs)
        if kind == "3D Surface":
            fig = go.Figure(go.Surface(x=xs, y=ys, z=M.values, colorscale=SEQ, colorbar=dict(thickness=10, len=.6),
                                       hovertemplate=f"{_short(x)} %{{x}}<br>{_short(y)} %{{y}}<br>{_short(z)} %{{z:,.2f}}<extra></extra>"))
            return _scene(_layout(fig, title, height, legend=False), _short(x), _short(y), _short(z), height)
        X, Y, Z, I, J, K, C = [], [], [], [], [], [], []
        for yi, yv in enumerate(ys):
            for xi, xv in enumerate(xs):
                h = M.loc[yv, xv]
                if pd.isna(h):
                    continue
                bx, by, bz, i, j, k = _box(xi, yi, .32, float(h))
                n = len(X)
                X += bx; Y += by; Z += bz; I += [n + a for a in i]; J += [n + a for a in j]; K += [n + a for a in k]; C += [float(h)] * 8
        fig = go.Figure(go.Mesh3d(x=X, y=Y, z=Z, i=I, j=J, k=K, intensity=C, colorscale=SEQ, flatshading=True, colorbar=dict(thickness=10, len=.6), hoverinfo="skip"))
        fig = _scene(_layout(fig, title, height, legend=False), _short(x), _short(y), _short(z), height)
        fig.update_layout(scene=dict(xaxis=dict(tickmode="array", tickvals=list(range(len(xs))), ticktext=xs), yaxis=dict(tickmode="array", tickvals=list(range(len(ys))), ticktext=ys)))
        return fig
    raise ValueError(f"Unknown chart type: {kind}")
