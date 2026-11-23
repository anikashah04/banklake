"""Function converter: legacy (Teradata-style) functions -> Athena (Trino) equivalents.

A registry maps each function name to a small converter that receives the already-converted argument
token lists. Rewriting is recursive, so nested calls such as ZEROIFNULL(SUM(NVL(x, 0))) are handled exactly.
"""
import re

from .parser import Tok, calls, match_paren, next_sig, render, split_args, split_top_word, tokenize


class Unsupported(Exception):
    pass


def _s(arg):
    return render(arg).strip()


def _need(args, n, name):
    if len(args) != n:
        raise Unsupported(f"{name}() with {len(args)} argument(s)")


# format-token translation for TO_DATE / TO_CHAR / TO_TIMESTAMP (Teradata/Oracle style -> Trino MySQL-style)
_FMT = [("YYYY", "%Y"), ("YY", "%y"), ("MONTH", "%M"), ("MON", "%b"), ("MM", "%m"), ("DD", "%d"),
        ("HH24", "%H"), ("HH12", "%h"), ("HH", "%H"), ("MI", "%i"), ("SS", "%s")]


def translate_format(lit: str) -> str:
    if not (lit.startswith("'") and lit.endswith("'")):
        raise Unsupported("date format must be a string literal")
    body, out, i = lit[1:-1], [], 0
    while i < len(body):
        for tok, rep in _FMT:
            if body[i:i + len(tok)].upper() == tok:
                out.append(rep)
                i += len(tok)
                break
        else:
            if body[i].isalpha():
                raise Unsupported(f"date format element {body[i:i+3]!r}")
            out.append(body[i].replace("%", "%%"))
            i += 1
    return "'" + "".join(out) + "'"


def f_zeroifnull(a):
    _need(a, 1, "ZEROIFNULL")
    return f"COALESCE({_s(a[0])}, 0)", "ZEROIFNULL(x) -> COALESCE(x, 0)"


def f_nullifzero(a):
    _need(a, 1, "NULLIFZERO")
    return f"NULLIF({_s(a[0])}, 0)", "NULLIFZERO(x) -> NULLIF(x, 0)"


def f_nvl(a):
    _need(a, 2, "NVL")
    return f"COALESCE({_s(a[0])}, {_s(a[1])})", "NVL(a, b) -> COALESCE(a, b)"


def f_nvl2(a):
    _need(a, 3, "NVL2")
    return f"CASE WHEN {_s(a[0])} IS NOT NULL THEN {_s(a[1])} ELSE {_s(a[2])} END", "NVL2(a, b, c) -> CASE WHEN"


def f_index(a):
    _need(a, 2, "INDEX")
    return f"strpos({_s(a[0])}, {_s(a[1])})", "INDEX(s, sub) -> strpos(s, sub)"


def f_position(a):
    if len(a) == 1:
        sp = split_top_word(a[0], "IN")
        if sp:
            return f"strpos({render(sp[1]).strip()}, {render(sp[0]).strip()})", "POSITION(x IN y) -> strpos(y, x)"
    raise Unsupported("POSITION() form")


def f_length(a):
    _need(a, 1, "CHARACTERS")
    return f"length({_s(a[0])})", "CHARACTERS/CHAR_LENGTH -> length"


def f_oreplace(a):
    if len(a) == 2:
        return f"replace({_s(a[0])}, {_s(a[1])})", "OREPLACE -> replace"
    _need(a, 3, "OREPLACE")
    return f"replace({_s(a[0])}, {_s(a[1])}, {_s(a[2])})", "OREPLACE -> replace"


def f_otranslate(a):
    _need(a, 3, "OTRANSLATE")
    return f"translate({_s(a[0])}, {_s(a[1])}, {_s(a[2])})", "OTRANSLATE -> translate"


def f_add_months(a):
    _need(a, 2, "ADD_MONTHS")
    return f"date_add('month', {_s(a[1])}, {_s(a[0])})", "ADD_MONTHS(d, n) -> date_add('month', n, d)"


def f_to_date(a):
    _need(a, 2, "TO_DATE")
    return f"CAST(date_parse({_s(a[0])}, {translate_format(_s(a[1]))}) AS DATE)", "TO_DATE(s, fmt) -> CAST(date_parse(s, fmt) AS DATE)"


def f_to_timestamp(a):
    _need(a, 2, "TO_TIMESTAMP")
    return f"date_parse({_s(a[0])}, {translate_format(_s(a[1]))})", "TO_TIMESTAMP(s, fmt) -> date_parse(s, fmt)"


def f_to_char(a):
    _need(a, 2, "TO_CHAR")
    return f"date_format({_s(a[0])}, {translate_format(_s(a[1]))})", "TO_CHAR(d, fmt) -> date_format(d, fmt)"


def f_csum(a):
    _need(a, 2, "CSUM")
    return f"SUM({_s(a[0])}) OVER (ORDER BY {_s(a[1])} ROWS UNBOUNDED PRECEDING)", "CSUM(x, k) -> SUM(x) OVER (ORDER BY k ...)"


def _moving(fn):
    def f(a):
        _need(a, 3, fn.upper())
        n = _s(a[1])
        if not n.isdigit() or int(n) < 1:
            raise Unsupported(f"{fn.upper()} window width must be a positive integer literal")
        return (f"{fn}({_s(a[0])}) OVER (ORDER BY {_s(a[2])} ROWS BETWEEN {int(n) - 1} PRECEDING AND CURRENT ROW)",
                f"M{fn.upper()}(x, n, k) -> {fn}(x) OVER (... PRECEDING)")
    return f


def f_log(a):
    _need(a, 1, "LOG")
    return f"log10({_s(a[0])})", "LOG(x) -> log10(x)  (Teradata LOG is base 10)"


REGISTRY = {
    "ZEROIFNULL": f_zeroifnull, "NULLIFZERO": f_nullifzero, "NVL": f_nvl, "NVL2": f_nvl2, "INDEX": f_index,
    "POSITION": f_position, "CHARACTERS": f_length, "CHAR_LENGTH": f_length, "CHARACTER_LENGTH": f_length,
    "CHARS": f_length, "OREPLACE": f_oreplace, "OTRANSLATE": f_otranslate, "ADD_MONTHS": f_add_months,
    "TO_DATE": f_to_date, "TO_TIMESTAMP": f_to_timestamp, "TO_CHAR": f_to_char, "CSUM": f_csum,
    "MSUM": _moving("SUM"), "MAVG": _moving("AVG"), "LOG": f_log,
}


def _after_is_over(tokens, close_idx) -> bool:
    j = next_sig(tokens, close_idx)
    return j != -1 and tokens[j].is_word("OVER")


def rewrite(tokens, convert_type, applied: list, errors: list):
    """Recursively rewrite function calls and CAST types. Returns a new token list."""
    out, i, n = [], 0, len(tokens)
    while i < n:
        t = tokens[i]
        j = next_sig(tokens, i) if t.kind == "word" else -1
        if t.kind == "word" and j != -1 and tokens[j].text == "(":
            k = match_paren(tokens, j)
            if k != -1:
                name = t.text.upper()
                inner = tokens[j + 1:k]
                if name == "CAST":
                    out.extend(_cast(t, tokens, j, k, convert_type, applied, errors))
                    i = k + 1
                    continue
                conv_inner = rewrite(inner, convert_type, applied, errors)
                args = split_args(conv_inner)
                try:
                    if name == "RANK" and args and not _after_is_over(tokens, k):
                        text, rule = f"RANK() OVER (ORDER BY {', '.join(_s(x) for x in args)})", "RANK(col DESC) -> RANK() OVER (ORDER BY col DESC)"
                    elif name in REGISTRY:
                        text, rule = REGISTRY[name](args)
                    else:
                        text = rule = None
                except Unsupported as e:
                    errors.append(str(e))
                    text = rule = None
                if text is not None:
                    if rule not in applied:
                        applied.append(rule)
                    out.append(Tok("raw", text))
                else:
                    out.extend([t] + tokens[i + 1:j + 1] + conv_inner + [tokens[k]])
                i = k + 1
                continue
        out.append(t)
        i += 1
    return out


def _cast(name_tok, tokens, j, k, convert_type, applied, errors):
    from .datatypes import Unsupported as TypeUnsupported
    inner = tokens[j + 1:k]
    sp = split_top_word(inner, "AS")
    if not sp:
        errors.append("CAST without AS")
        return [name_tok] + tokens[j:k + 1]
    expr = rewrite(sp[0], convert_type, applied, errors)
    while expr and expr[-1].kind == "ws":
        expr.pop()
    type_text = render(sp[1]).strip()
    try:
        new_type, note = convert_type(type_text)
    except TypeUnsupported as e:
        errors.append(str(e))
        return [name_tok, Tok("op", "(")] + expr + [Tok("word", " AS "), Tok("raw", type_text), Tok("op", ")")]
    if new_type.upper().replace(" ", "") != type_text.upper().replace(" ", ""):
        rule = f"CAST type {type_text} -> {new_type}"
        if rule not in applied:
            applied.append(rule)
    if note and note not in applied:
        applied.append(note)
    return [name_tok, Tok("op", "(")] + expr + [Tok("raw", " AS " + new_type), Tok("op", ")")]


def rewrite_keywords(tokens, applied):
    """Token-level rewrites that are not function calls: SEL -> SELECT, a MOD b -> mod(a, b)."""
    out = list(tokens)
    for i, t in enumerate(out):
        if t.is_word("SEL"):
            out[i] = Tok("word", "SELECT")
            if "SEL -> SELECT" not in applied:
                applied.append("SEL -> SELECT")
    i = 0
    while i < len(out):
        if out[i].is_word("MOD"):
            p = max((x for x in range(i) if out[x].kind not in ("ws",)), default=-1)
            q = next_sig(out, i)
            if p != -1 and q != -1 and out[p].kind in ("word", "number") and out[q].kind in ("word", "number"):
                out[p:q + 1] = [Tok("raw", f"mod({out[p].text}, {out[q].text})")]
                if "a MOD b -> mod(a, b)" not in applied:
                    applied.append("a MOD b -> mod(a, b)")
                continue
        i += 1
    return out


def rewrite_top(tokens, applied, errors):
    """SELECT TOP n ... -> SELECT ... LIMIT n  (outermost SELECT only)."""
    depth, first_select = 0, None
    for i, t in enumerate(tokens):
        if t.kind == "op" and t.text == "(":
            depth += 1
        elif t.kind == "op" and t.text == ")":
            depth -= 1
        elif depth == 0 and t.is_word("SELECT") and first_select is None:
            first_select = i
    if first_select is None:
        return tokens
    j = next_sig(tokens, first_select)
    if j == -1 or not tokens[j].is_word("TOP"):
        return tokens
    q = next_sig(tokens, j)
    if q == -1 or tokens[q].kind != "number":
        return tokens
    depth = 0
    for t in tokens:
        if t.kind == "op" and t.text == "(":
            depth += 1
        elif t.kind == "op" and t.text == ")":
            depth -= 1
        elif depth == 0 and t.is_word("UNION", "INTERSECT", "EXCEPT", "MINUS"):
            errors.append("TOP n combined with a set operation")
            return tokens
    n = tokens[q].text
    new = tokens[:j] + tokens[q + 1:]
    while new and new[-1].kind in ("ws", "lcomment", "bcomment") or (new and new[-1].text == ";"):
        new.pop()
    new += [Tok("raw", f"\nLIMIT {n}")]
    applied.append("TOP n -> LIMIT n")
    return new


__all__ = ["rewrite", "rewrite_keywords", "rewrite_top", "REGISTRY", "Unsupported", "calls", "tokenize"]
