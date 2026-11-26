import os
import sys
import tempfile
from pathlib import Path

# isolated data dir for the whole test session (must be set before importing banklake)
os.environ["BANKLAKE_HOME"] = tempfile.mkdtemp(prefix="banklake_test_")
os.environ.setdefault("BANKLAKE_SOURCE_SCALE", "small")   # tests seed the source databases small; the app uses the large "source" scale
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import json  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def lake():
    """Generate small synthetic data and run the whole pipeline once."""
    from banklake import datagen, meta, orchestrator
    from banklake.common import RAW
    meta.init_db()
    datagen.generate_full("small")
    res = orchestrator.run(retry_sleep=0)
    assert set(res["status"].values()) == {"SUCCESS"}, res
    return json.loads((RAW / "_anomaly_manifest.json").read_text())


def silver(entity):
    import duckdb
    from banklake.common import LAKE
    return duckdb.connect().execute(f"SELECT * FROM read_parquet('{LAKE}/silver/{entity}/*.parquet')").fetchdf()


def quarantine_counts(entity):
    """Rejection reasons from both Silver stages: Silver Merge (missing primary key) and Silver Transform."""
    import duckdb
    from banklake.common import LAKE
    con = duckdb.connect()
    counts = dict(con.execute(
        f"SELECT reject_reason, count(*) FROM read_parquet('{LAKE}/quarantine/silver_{entity}/*.parquet') GROUP BY 1").fetchall())
    merge_dir = LAKE / "quarantine" / f"silver_merge_{entity}"
    if any(merge_dir.glob("*.parquet")):
        for reason, n in con.execute(f"SELECT reject_reason, count(*) FROM read_parquet('{merge_dir}/*.parquet') GROUP BY 1").fetchall():
            reason = f"NULL_{entity.upper()}_ID" if reason == "NULL_PRIMARY_KEY" else reason
            counts[reason] = counts.get(reason, 0) + n
    return counts


RD = "2026-11-25"


@pytest.fixture(scope="session")
def bronze_all(lake):
    """The 17 source-only Bronze tables come from a real small ingestion run in its own home (so the shared core Bronze is untouched)."""
    import shutil
    import subprocess

    from banklake.common import LAKE
    from banklake.schemas import SOURCE_ONLY
    home = tempfile.mkdtemp(prefix="banklake_ing_")
    code = ("from datetime import date\nfrom banklake import meta, sources, ingest\nmeta.init_db(); sources.ensure_sources()\n"
            "assert ingest.run(date(2026, 7, 1))['status'] == 'SUCCESS'")
    env = {**os.environ, "BANKLAKE_HOME": home, "BANKLAKE_SOURCE_SCALE": "small", "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=900)
    assert r.returncode == 0, (r.stdout + r.stderr)[-1500:]
    for e in SOURCE_ONLY:
        shutil.copytree(Path(home) / "data" / "lake" / "bronze" / e, LAKE / "bronze" / e, dirs_exist_ok=True)
    return True


@pytest.fixture(scope="module")
def loaded(request, lake, bronze_all):
    """Run every procedure of the three layers for one report date, in pipeline order."""
    from banklake import meta
    from banklake.transform import catalog, engine
    meta.init_db()
    res = {}
    for layer in catalog.LAYERS:
        for p in catalog.procs_for(layer):
            res[p["proc_id"]] = engine.run_proc(p["proc_id"], RD, getattr(request.module, "ORDER", "ORD-TEST-1"))
    return res


