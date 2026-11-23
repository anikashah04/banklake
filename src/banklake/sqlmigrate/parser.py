"""SQL parser layer: a tokenizer plus helpers for balanced parentheses and argument lists.

This is not a full SQL grammar. It is exactly as much parsing as the converters need to be safe: string
literals, quoted identifiers and comments are recognised, so a rewrite can never touch text inside them, and
function calls are found with real parenthesis matching (arbitrary nesting) instead of regular expressions.
"""
import re
from dataclasses import dataclass

_TOKEN = re.compile(r"""
  (?P<ws>\s+)
 |(?P<lcomment>--[^\n]*)
 |(?P<bcomment>/\*.*?\*/)
 |(?P<string>'(?:[^']|'')*')
 |(?P<qident>"(?:[^"]|"")*")
 |(?P<number>\d+(?:\.\d+)?)
 |(?P<word>[A-Za-z_][A-Za-z_0-9$]*)
 |(?P<op><>|!=|<=|>=|\|\||\*\*|[-+*/%=<>.,;()])
 |(?P<other>.)
""", re.X | re.S)

SKIP = ("ws", "lcomment", "bcomment")


@dataclass
class Tok:
    kind: str
    text: str

    def is_word(self, *names) -> bool:
        return self.kind == "word" and (not names or self.text.upper() in names)


def tokenize(sql: str) -> list[Tok]:
    return [Tok(m.lastgroup, m.group()) for m in _TOKEN.finditer(sql)]


def render(tokens: list[Tok]) -> str:
    return "".join(t.text for t in tokens)


def next_sig(tokens, i: int) -> int:
    """Index of the next token after i that is not whitespace/comment, or -1."""
    for j in range(i + 1, len(tokens)):
        if tokens[j].kind not in SKIP:
            return j
    return -1


def prev_sig(tokens, i: int) -> int:
    for j in range(i - 1, -1, -1):
        if tokens[j].kind not in SKIP:
            return j
    return -1


def match_paren(tokens, i: int) -> int:
    """Index of the ')' that closes the '(' at i, or -1 if unbalanced."""
    depth = 0
    for j in range(i, len(tokens)):
        if tokens[j].kind == "op":
            if tokens[j].text == "(":
                depth += 1
            elif tokens[j].text == ")":
                depth -= 1
                if depth == 0:
                    return j
    return -1


def split_args(tokens: list[Tok]) -> list[list[Tok]]:
    """Split the tokens between a call's parentheses at top-level commas."""
    args, cur, depth = [], [], 0
    for t in tokens:
        if t.kind == "op" and t.text == "(":
            depth += 1
        elif t.kind == "op" and t.text == ")":
            depth -= 1
        if t.kind == "op" and t.text == "," and depth == 0:
            args.append(cur)
            cur = []
        else:
            cur.append(t)
    args.append(cur)
    return [a for a in args] if any(render(a).strip() for a in args) else []


def split_top_word(tokens: list[Tok], word: str):
    """Split at the first top-level occurrence of a keyword; returns (before, after) or None."""
    depth = 0
    for i, t in enumerate(tokens):
        if t.kind == "op" and t.text == "(":
            depth += 1
        elif t.kind == "op" and t.text == ")":
            depth -= 1
        elif depth == 0 and t.is_word(word):
            return tokens[:i], tokens[i + 1:]
    return None


def calls(tokens):
    """Yield (name_index, open_paren_index, close_paren_index) for every  word(  ...  )  in the token list."""
    for i, t in enumerate(tokens):
        if t.kind == "word":
            j = next_sig(tokens, i)
            if j != -1 and tokens[j].kind == "op" and tokens[j].text == "(":
                k = match_paren(tokens, j)
                if k != -1:
                    yield i, j, k


def statement_kind(tokens) -> str:
    for t in tokens:
        if t.kind not in SKIP and not (t.kind == "op" and t.text in ("(", ";")):
            return t.text.upper() if t.kind == "word" else t.text
    return ""
