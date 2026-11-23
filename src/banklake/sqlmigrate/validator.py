"""Query validator: checks that the converted SQL parses as Athena (Trino) SQL.

Uses the sqlglot parser with the Trino dialect when it is installed. This proves syntax only; semantic
validation happens by executing the query and comparing results (comparator.py), and, when AWS is configured,
by Athena itself (aws/ scripts).
"""
from dataclasses import dataclass, field


@dataclass
class SyntaxReport:
    ok: bool
    errors: list = field(default_factory=list)
    engine: str = "sqlglot(trino)"


def check_syntax(sql: str) -> SyntaxReport:
    depth = 0
    for ch in sql:
        depth += (ch == "(") - (ch == ")")
        if depth < 0:
            return SyntaxReport(False, ["unbalanced parentheses"], "bracket check")
    if depth:
        return SyntaxReport(False, ["unbalanced parentheses"], "bracket check")
    try:
        import sqlglot
        from sqlglot.errors import ParseError
    except ImportError:
        return SyntaxReport(True, ["sqlglot not installed: only bracket balance was checked"], "bracket check")
    try:
        sqlglot.parse_one(sql.strip().rstrip(";"), read="trino")
    except ParseError as e:
        return SyntaxReport(False, [str(e).splitlines()[0][:200]])
    except Exception as e:  # noqa: BLE001 - tokenizer errors etc.
        return SyntaxReport(False, [f"{type(e).__name__}: {str(e)[:200]}"])
    return SyntaxReport(True)
