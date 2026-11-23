"""Conversion pipeline: tokenize -> detect unsupported -> keyword/function/type rewrites -> TOP->LIMIT -> schema mapping -> syntax check."""
from dataclasses import dataclass, field

from . import functions
from .datatypes import convert_type
from .parser import render, tokenize
from .schema import SchemaMapper
from .unsupported import detect
from .validator import SyntaxReport, check_syntax


@dataclass
class Conversion:
    legacy: str
    converted: str
    applied: list = field(default_factory=list)
    unsupported: list = field(default_factory=list)
    syntax: SyntaxReport | None = None

    @property
    def status(self):
        return "UNSUPPORTED" if self.unsupported else "CONVERTED"


def convert(legacy_sql: str, mapper: SchemaMapper | None = None) -> Conversion:
    mapper = mapper or SchemaMapper()
    tokens = [t for t in tokenize(legacy_sql) if t.kind not in ("lcomment", "bcomment")]
    while tokens and (tokens[-1].kind == "ws" or tokens[-1].text == ";"):
        tokens.pop()
    while tokens and tokens[0].kind == "ws":
        tokens.pop(0)
    res = Conversion(legacy_sql, render(tokens))
    res.unsupported = detect(tokens)
    errors: list = []
    tokens = functions.rewrite_keywords(tokens, res.applied)
    tokens = functions.rewrite(tokens, convert_type, res.applied, errors)
    tokens = functions.rewrite_top(tokens, res.applied, errors)
    tokens, notes = mapper.apply(tokens)
    res.applied += notes
    for e in errors:
        if e not in res.unsupported:
            res.unsupported.append(e)
    res.converted = render(tokens) + ";"
    if not res.unsupported:
        res.syntax = check_syntax(res.converted)
        if not res.syntax.ok:
            res.unsupported.append("converted SQL failed Athena syntax check: " + "; ".join(res.syntax.errors))
    return res
