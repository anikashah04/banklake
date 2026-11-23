"""Teradata stored procedure -> Athena statements + an EMR (PySpark) script.

Deliberately modest: a procedure is split into its SQL statements, each is converted with the same tested converter used
for single queries, and procedural parts (DECLARE, SET, IF, WHILE, LOOP, CALL, handlers) are reported as 'needs manual
rewrite' instead of being guessed. The EMR script is generated from the converted statements (Trino -> Spark SQL via
sqlglot) and is only syntax-checked; it has not been run on EMR."""
import re
from dataclasses import dataclass, field

import sqlglot

from .converter import convert
from .parser import tokenize

PROCEDURAL = ("DECLARE", "SET", "IF", "ELSEIF", "ELSE", "WHILE", "FOR", "LOOP", "REPEAT", "CALL", "LEAVE", "ITERATE",
              "OPEN", "CLOSE", "FETCH", "SIGNAL", "RESIGNAL", "EXIT", "CONTINUE", "END")
HEADER = re.compile(r"^\s*(?:CREATE|REPLACE)\s+PROCEDURE\s+([\w.\"]+)\s*\((.*?)\)\s*(?:BEGIN\b|$)", re.I | re.S)


@dataclass
class ProcStatement:
    n: int
    legacy: str
    kind: str                 # SQL | PROCEDURAL
    converted: str = ""
    spark: str = ""
    status: str = ""          # CONVERTED | UNSUPPORTED | MANUAL
    notes: list = field(default_factory=list)


@dataclass
class ProcResult:
    name: str
    params: list
    statements: list
    script: str = ""

    @property
    def counts(self):
        c = {"CONVERTED": 0, "UNSUPPORTED": 0, "MANUAL": 0}
        for s in self.statements:
            c[s.status] += 1
        return c


def _strip_comments(text):
    return "".join(t.text if t.kind not in ("lcomment", "bcomment") else " " for t in tokenize(text))


def split_statements(body):
    """Split on semicolons that are outside quotes/comments (the tokenizer knows which are which)."""
    out, cur = [], []
    for t in tokenize(body):
        if t.kind in ("lcomment", "bcomment"):
            continue
        if t.text == ";" and t.kind != "string":
            s = "".join(cur).strip()
            if s:
                out.append(s)
            cur = []
        else:
            cur.append(t.text)
    s = "".join(cur).strip()
    if s:
        out.append(s)
    return out


def _spark(sql):
    stmt = sql.rstrip().rstrip(";")
    try:
        return sqlglot.transpile(stmt, read="trino", write="spark")[0]
    except Exception as e:  # noqa: BLE001
        return f"-- could not translate to Spark SQL: {e}"


DELETE_RE = re.compile(r"^DEL(?:ETE)?\s+(?:FROM\s+)?([\w.\"]+)(?:\s+ALL)?\s*(WHERE\b.*)?$", re.I | re.S)
INSERT_RE = re.compile(r"^INS(?:ERT)?\s+INTO\s+([\w.\"]+)\s*(\([^)]*\))?\s*((?:SEL|SELECT)\b.*)$", re.I | re.S)


def _check_trino(sql):
    try:
        sqlglot.parse_one(sql.rstrip().rstrip(";"), read="trino")
        return []
    except Exception as e:  # noqa: BLE001
        return ["converted SQL failed Athena syntax check: " + str(e).splitlines()[0]]


def _convert_dml(raw):
    """DELETE and INSERT ... SELECT. The table name and the SELECT go through the normal converter, so function, type and schema
    rewrites are identical to a plain query. Returns a builder taking the statement number, or None for other statements."""
    m = DELETE_RE.match(raw.strip())
    kind = "DELETE"
    if not m:
        m, kind = INSERT_RE.match(raw.strip()), "INSERT"
    if not m:
        return None

    def build(n):
        notes, problems = [], []
        if kind == "DELETE":
            c = convert(f"SELECT * FROM {m.group(1)} {m.group(2) or ''}")
            out = re.sub(r"^SELECT\s+\*\s+FROM", "DELETE FROM", c.converted.strip(), flags=re.I)
            notes = c.applied + ["DELETE ... ALL -> DELETE FROM (no WHERE)"] if re.search(r"\bALL\s*$", raw.strip(), re.I) or "ALL" in raw.upper().split() else c.applied
            notes.append("row-level DELETE needs an Iceberg table in Athena/Spark (plain Hive/Parquet tables cannot delete rows)")
        else:
            tgt = convert(f"SELECT * FROM {m.group(1)}")
            sel = convert(m.group(3))
            tname = re.sub(r"^SELECT\s+\*\s+FROM\s+", "", tgt.converted.strip().rstrip(";"), flags=re.I)
            out = f"INSERT INTO {tname} {m.group(2) or ''}\n{sel.converted}".replace("  ", " ")
            c = sel
            notes = tgt.applied + sel.applied
        problems = list(c.unsupported) + _check_trino(out)
        s = ProcStatement(n, raw, "SQL", converted=out, notes=notes, status="UNSUPPORTED" if problems else "CONVERTED")
        s.notes += ["UNSUPPORTED: " + x for x in problems]
        if not problems:
            s.spark = _spark(out)
        return s
    return build


def convert_procedure(text):
    text = text.strip()
    m = HEADER.match(text)
    name, params = "unnamed_procedure", []
    if m:
        name = m.group(1).split(".")[-1].strip('"').lower()
        params = [p.strip() for p in m.group(2).split(",") if p.strip()]
        text = text[m.end():]
    text = re.sub(r"^\s*BEGIN\b", "", text, flags=re.I)
    text = re.sub(r"\bEND\s*;?\s*$", "", text.strip(), flags=re.I)
    stmts = []
    for i, raw in enumerate(split_statements(text), 1):
        first = raw.split(None, 1)[0].upper()
        if first in PROCEDURAL:
            stmts.append(ProcStatement(i, raw, "PROCEDURAL", status="MANUAL",
                                       notes=[f"{first} is procedural logic; it has no direct Athena SQL form. Rewrite it in the EMR script or the orchestrator (Step Functions)."]))
            continue
        dml = _convert_dml(raw)
        if dml is not None:
            stmts.append(dml(i))
            continue
        c = convert(raw)
        s = ProcStatement(i, raw, "SQL", converted=c.converted, notes=list(c.applied), status=c.status)
        if c.unsupported:
            s.notes += ["UNSUPPORTED: " + u for u in c.unsupported]
        else:
            s.spark = _spark(c.converted)
        stmts.append(s)
    res = ProcResult(name, params, stmts)
    res.script = build_emr_script(res)
    return res


def build_emr_script(res: ProcResult):
    L = [f'"""EMR (PySpark) job generated from the Teradata procedure {res.name}.',
         "Generated by BankLake's procedure converter. The SQL was converted and syntax-checked; this script has NOT been run on EMR.",
         'Statements marked MANUAL / UNSUPPORTED need a person to finish them. Review before use."""',
         "import sys", "from pyspark.sql import SparkSession", "",
         f'spark = SparkSession.builder.appName("{res.name}").getOrCreate()']
    for p in res.params:
        pn = p.split()[-2] if p.upper().startswith(("IN ", "OUT ", "INOUT ")) else p.split()[0]
        L.append(f'{pn} = sys.argv[{res.params.index(p) + 1}] if len(sys.argv) > {res.params.index(p) + 1} else None  # procedure parameter: {p}')
    L.append("")
    for s in res.statements:
        L.append(f"# --- step {s.n}: {s.status}")
        if s.status == "CONVERTED":
            L.append(f'spark.sql("""\n{s.spark}\n""")' + (".show()" if s.converted.lstrip().upper().startswith("SELECT") else ""))
        else:
            L += ["# ORIGINAL (Teradata):"] + ["#   " + ln for ln in s.legacy.splitlines()] + ["# TODO: " + n for n in s.notes[:1]]
        L.append("")
    L.append("spark.stop()")
    return "\n".join(L) + "\n"


def script_compiles(script):
    try:
        compile(script, "emr_job.py", "exec")
        return True, ""
    except SyntaxError as e:
        return False, str(e)
