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
