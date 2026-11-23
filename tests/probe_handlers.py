"""Tiny pipeline handlers used to prove that metadata - not code - controls execution."""
CALLS: list[str] = []
_attempts: dict[str, int] = {}


def ok(spark, m, ctx):
    CALLS.append(m["pipeline_id"])
    return dict(rows_in=1, rows_out=1, rows_rejected=0)


def fail_once(spark, m, ctx):
    n = _attempts[m["pipeline_id"]] = _attempts.get(m["pipeline_id"], 0) + 1
    CALLS.append(m["pipeline_id"])
    if n == 1:
        raise RuntimeError("transient failure")
    return dict(rows_in=1, rows_out=1, rows_rejected=0)


def always_fail(spark, m, ctx):
    CALLS.append(m["pipeline_id"])
    raise RuntimeError("permanent failure")


def schema_fail(spark, m, ctx):
    CALLS.append(m["pipeline_id"])
    raise ValueError("Schema mismatch in x.csv: missing columns ['a']")


def missing_input(spark, m, ctx):
    CALLS.append(m["pipeline_id"])
    raise FileNotFoundError("No such file or directory: raw/x")
