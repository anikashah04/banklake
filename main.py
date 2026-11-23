"""BankLake command line.  python main.py --help"""
import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from banklake import meta  # noqa: E402
from banklake.common import DATA, ROOT  # noqa: E402


def cmd_reset(_):
    if DATA.exists():
        shutil.rmtree(DATA)
    meta.init_db()
    print("Reset done: data/ wiped, metadata re-seeded.")


def cmd_generate(a):
    from banklake import datagen
    if a.incremental:
        print(json.dumps(datagen.generate_incremental(deletes=a.deletes), indent=2))
    else:
        print(json.dumps(datagen.generate_full(a.scale), indent=2))


def cmd_run(a):
    from banklake import orchestrator
    res = orchestrator.run(layers=a.layers, pipelines=a.pipelines, full_refresh=a.full_refresh)
    print(f"run_id = {res['run_id']}")
    for pid, st in res["status"].items():
        print(f"  {st:8} {pid}")
    sys.exit(0 if all(s in ("SUCCESS", "SKIPPED") for s in res["status"].values()) else 1)


def cmd_reload(a):
    from banklake import orchestrator
    res = orchestrator.reload(a.pipelines, downstream=not a.no_downstream)
    print(f"reload run_id = {res['run_id']}")
    for pid, st in res["status"].items():
        print(f"  {st:8} {pid}")
    sys.exit(0 if all(s in ("SUCCESS", "SKIPPED") for s in res["status"].values()) else 1)


def cmd_reconcile(a):
    from banklake import reconcile
    res = reconcile.run_reconciliation()
    print(reconcile.format_report(res))


def cmd_trace(a):
    from banklake import trace
    for e in (a.entity or ["customer", "account", "transaction", "deposit", "loan", "branch"]):
        print(trace.format_trace(trace.trace(e)))
        print()


def cmd_inject(a):
    from banklake import reconcile
    print(json.dumps(reconcile.inject_discrepancy(a.entity, a.mode, a.n), indent=2))


def cmd_lineage(a):
    from banklake import lineage
    out = ROOT / "docs" / "GOLD_LINEAGE.md"
    out.write_text(lineage.to_markdown())
    print(f"wrote {out}")


def cmd_guide(a):
    from banklake import guide
    out = ROOT / "docs" / "USER_GUIDE.md"
    out.write_text(guide.to_markdown())
    print(f"wrote {out}")


def cmd_sqlmigrate(a):
    from banklake import sqlmigrate
    print(json.dumps(sqlmigrate.run_all(), indent=2, default=str))


def cmd_demo(a):
    """Everything, start to finish."""
    cmd_reset(a)
    a.incremental = False
    cmd_generate(a)
    a.layers = a.pipelines = None
    a.full_refresh = False
    try:
        cmd_run(a)
    except SystemExit as e:
        if e.code:
            raise
    cmd_reconcile(a)
    cmd_sqlmigrate(a)


def main():
    p = argparse.ArgumentParser(description="BankLake - banking data lakehouse (synthetic data only)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init").set_defaults(fn=lambda a: (meta.init_db(), print("metadata DB ready")))
    sub.add_parser("reset", help="wipe data/ and start clean").set_defaults(fn=cmd_reset)
    g = sub.add_parser("generate", help="create synthetic source data")
    g.add_argument("--scale", choices=["small", "full"], default="full")
    g.add_argument("--incremental", action="store_true", help="add a new incremental batch")
    g.add_argument("--deletes", action="store_true", help="with --incremental: include delete (CDC) records")
    g.set_defaults(fn=cmd_generate)
    r = sub.add_parser("run", help="run pipelines from metadata")
    r.add_argument("--layers", nargs="*", choices=["bronze", "silver_merge", "silver", "gold"])
    r.add_argument("--pipelines", nargs="*")
    r.add_argument("--full-refresh", action="store_true")
    r.set_defaults(fn=cmd_run)
    rl = sub.add_parser("reload", help="rebuild pipelines from scratch (full refresh) plus everything downstream")
    rl.add_argument("--pipelines", nargs="+", required=True)
    rl.add_argument("--no-downstream", action="store_true", help="only the named pipelines")
    rl.set_defaults(fn=cmd_reload)
    sub.add_parser("reconcile", help="data quality + source-to-target reconciliation").set_defaults(fn=cmd_reconcile)
    tr = sub.add_parser("trace", help="explain, layer by layer, why source records are missing from Silver")
    tr.add_argument("--entity", nargs="*")
    tr.set_defaults(fn=cmd_trace)
    i = sub.add_parser("inject", help="inject a discrepancy into Silver to demo detection")
    i.add_argument("--entity", default="account")
    i.add_argument("--mode", choices=["delete", "duplicate", "alter"], default="delete")
    i.add_argument("--n", type=int, default=25)
    i.set_defaults(fn=cmd_inject)
    sub.add_parser("guide", help="write docs/USER_GUIDE.md").set_defaults(fn=cmd_guide)
    sub.add_parser("lineage", help="write docs/GOLD_LINEAGE.md").set_defaults(fn=cmd_lineage)
    sub.add_parser("sqlmigrate", help="legacy SQL -> Athena SQL + validation").set_defaults(fn=cmd_sqlmigrate)
    d = sub.add_parser("demo", help="reset, generate, run all, reconcile, SQL migration")
    d.add_argument("--scale", choices=["small", "full"], default="small")
    d.set_defaults(fn=cmd_demo)
    a = p.parse_args()
    meta.init_db()
    a.fn(a)


if __name__ == "__main__":
    main()
