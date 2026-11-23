"""Result comparator: row counts, key sets, numeric totals and record-level differences between two result sets."""
from datetime import date, datetime
from decimal import Decimal

TOL = 0.01


def _num(x):
    return isinstance(x, (int, float, Decimal)) and not isinstance(x, bool)


def _norm(v):
    if _num(v):
        return float(v)
    if isinstance(v, datetime):
        return v
    return v


def _same(a, b, tol=TOL):
    a, b = _norm(a), _norm(b)
    if a is None or b is None:
        return a is None and b is None
    if _num(a) and _num(b):
        return abs(a - b) <= tol
    return a == b


def compare(got, ref, key_idx=(0,), tol=TOL, max_diffs=5) -> dict:
    """got/ref: lists of tuples. key_idx: column positions that identify a record."""
    out = dict(rows_got=len(got), rows_ref=len(ref), row_count_match=len(got) == len(ref),
               missing_in_got=[], extra_in_got=[], mismatches=[], numeric_sums=[], duplicate_keys=False)
    key = lambda r: tuple(str(_norm(r[i])) for i in key_idx)  # noqa: E731
    g, r_ = {}, {}
    for row in got:
        if key(row) in g:
            out["duplicate_keys"] = True
        g[key(row)] = row
    for row in ref:
        r_[key(row)] = row
    out["missing_in_got"] = sorted(set(r_) - set(g))[:max_diffs]
    out["extra_in_got"] = sorted(set(g) - set(r_))[:max_diffs]
    bad = 0
    for k in sorted(set(g) & set(r_)):
        ga, ra = g[k], r_[k]
        if len(ga) != len(ra) or not all(_same(x, y, tol) for x, y in zip(ga, ra)):
            bad += 1
            if len(out["mismatches"]) < max_diffs:
                out["mismatches"].append(dict(key=k, got=[str(x) for x in ga], ref=[str(x) for x in ra]))
    out["mismatch_count"] = bad
    if got and ref:
        for c in range(min(len(got[0]), len(ref[0]))):
            if all(_num(x[c]) or x[c] is None for x in got) and all(_num(x[c]) or x[c] is None for x in ref) and c not in key_idx:
                sg = sum(float(x[c]) for x in got if x[c] is not None)
                sr = sum(float(x[c]) for x in ref if x[c] is not None)
                out["numeric_sums"].append(dict(col=c, got=round(sg, 4), ref=round(sr, 4), ok=abs(sg - sr) <= max(tol, 1e-9 * abs(sr))))
    out["match"] = (out["row_count_match"] and not out["missing_in_got"] and not out["extra_in_got"]
                    and bad == 0 and not out["duplicate_keys"] and all(s["ok"] for s in out["numeric_sums"]))
    return out


def summarize(c: dict) -> str:
    s = f"rows {c['rows_got']} vs {c['rows_ref']}"
    if c["match"]:
        return s + f"; {len(c['numeric_sums'])} numeric totals equal; 0 record differences"
    parts = []
    if not c["row_count_match"]:
        parts.append("row count differs")
    if c["missing_in_got"]:
        parts.append(f"missing keys {c['missing_in_got']}")
    if c["extra_in_got"]:
        parts.append(f"extra keys {c['extra_in_got']}")
    if c.get("mismatch_count"):
        parts.append(f"{c['mismatch_count']} records differ, first {c['mismatches'][0]}")
    bad = [x for x in c["numeric_sums"] if not x["ok"]]
    if bad:
        parts.append(f"total differs in column {bad[0]['col']}: {bad[0]['got']} vs {bad[0]['ref']}")
    return s + "; " + "; ".join(parts)
