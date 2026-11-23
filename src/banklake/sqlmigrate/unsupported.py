"""Unsupported-syntax detector: constructs that cannot be converted safely are reported, never guessed.

Works on tokens, so a word inside a string literal or a comment is never flagged.
"""
from .parser import SKIP, calls, next_sig, prev_sig, statement_kind, tokenize

UNSUPPORTED_FUNCTIONS = {"INITCAP": "INITCAP() (no Athena equivalent)", "MEDIAN": "MEDIAN() (use approx_percentile deliberately)",
                         "HASHROW": "HASHROW() (Teradata hash)", "HASHBUCKET": "HASHBUCKET() (Teradata hash)",
                         "HASHAMP": "HASHAMP() (Teradata hash)", "STRTOK": "STRTOK()", "RANDOM": "RANDOM(a, b)",
                         "STRTOK_SPLIT_TO_TABLE": "STRTOK_SPLIT_TO_TABLE()"}
SINGLE_WORDS = {"QUALIFY": "QUALIFY clause", "VOLATILE": "VOLATILE table", "MULTISET": "MULTISET table",
                "CASESPECIFIC": "CASESPECIFIC / NOT CASESPECIFIC", "PIVOT": "PIVOT", "UNPIVOT": "UNPIVOT"}
SUPPORTED_STATEMENTS = {"SELECT", "SEL", "WITH"}
TYPE_WORDS = {"DATE", "INTEGER", "INT", "VARCHAR", "CHAR", "TIMESTAMP", "DECIMAL", "FLOAT", "BIGINT", "SMALLINT", "BYTEINT", "TIME"}


def detect(tokens) -> list[str]:
    found: list[str] = []

    def add(msg):
        if msg not in found:
            found.append(msg)

    kind = statement_kind(tokens)
    if kind and kind not in SUPPORTED_STATEMENTS:
        add(f"statement type {kind} (only SELECT queries are converted)")
    sig = [(i, t) for i, t in enumerate(tokens) if t.kind not in SKIP]
    depth = 0
    for n, (i, t) in enumerate(sig):
        if t.kind == "op" and t.text == "(":
            depth += 1
        elif t.kind == "op" and t.text == ")":
            depth -= 1
        if t.kind != "word":
            continue
        w = t.text.upper()
        nxt = sig[n + 1][1] if n + 1 < len(sig) else None
        nxt2 = sig[n + 2][1] if n + 2 < len(sig) else None
        if w in SINGLE_WORDS:
            add(SINGLE_WORDS[w])
        elif w == "SAMPLE" and nxt is not None and nxt.kind == "number":
            add("SAMPLE clause")
        elif w == "COLLECT" and nxt is not None and nxt.is_word("STATISTICS", "STATS", "STAT"):
            add("COLLECT STATISTICS")
        elif w == "PERIOD" and nxt is not None and nxt.text == "(":
            add("PERIOD data type")
        elif w == "FORMAT" and nxt is not None and nxt.kind == "string":
            add("FORMAT phrase")
        elif w == "RESET" and nxt is not None and nxt.is_word("WHEN"):
            add("RESET WHEN (window)")
        elif w == "TOP" and prev_is_select(tokens, i):
            if depth > 0:
                add("TOP n inside a subquery")
            elif nxt2 is not None and nxt2.is_word("PERCENT") or (len(sig) > n + 3 and sig[n + 3][1].is_word("PERCENT")):
                add("TOP n PERCENT")
        elif w == "WITH" and nxt is not None and nxt.is_word("TIES"):
            add("WITH TIES")
        elif w in UNSUPPORTED_FUNCTIONS and nxt is not None and nxt.text == "(":
            add(UNSUPPORTED_FUNCTIONS[w])
    # Teradata type attribute cast:  'x' (DATE)   expr (INTEGER)
    for i, t in enumerate(tokens):
        if t.kind == "op" and t.text == "(":
            j = next_sig(tokens, i)
            k = next_sig(tokens, j) if j != -1 else -1
            p = prev_sig(tokens, i)
            if (j != -1 and k != -1 and tokens[j].is_word(*TYPE_WORDS) and tokens[k].text == ")"
                    and p != -1 and tokens[p].kind in ("string", "number", "qident")):
                add("Teradata type-attribute cast  expr (TYPE)")
    return found


def prev_is_select(tokens, i) -> bool:
    p = prev_sig(tokens, i)
    return p != -1 and tokens[p].is_word("SELECT", "SEL")


def detect_sql(sql: str) -> list[str]:
    return detect(tokenize(sql))


__all__ = ["detect", "detect_sql", "calls"]
