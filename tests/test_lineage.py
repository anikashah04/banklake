"""Gold lineage documentation is verified against the real tables, so it cannot drift."""
import duckdb

from banklake import lineage
from banklake.common import LAKE


def _cols(layer, name):
    return [r[0] for r in duckdb.connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{LAKE}/{layer}/{name}/*.parquet')").fetchall()]


def test_documented_gold_columns_equal_real_gold_columns(lake):
    for table, (grain, cols) in lineage.GOLD_LINEAGE.items():
        assert sorted(cols) == sorted(_cols("gold", table)), table


def test_every_documented_silver_source_column_exists(lake):
    silver = {}
    for table, (grain, cols) in lineage.GOLD_LINEAGE.items():
        for col, (logic, sources) in cols.items():
            assert sources and logic
            for s in sources:
                ent, c = s.split(".", 1)
                silver.setdefault(ent, _cols("silver", ent))
                assert c in silver[ent], f"{table}.{col}: silver {s} does not exist"


def test_table_lineage_matches_pipeline_metadata_and_documented_sources(lake):
    live = lineage.table_lineage()
    assert set(live) == set(lineage.GOLD_LINEAGE)
    for table, (grain, cols) in lineage.GOLD_LINEAGE.items():
        used = {s.split(".")[0] for _, srcs in cols.values() for s in srcs}
        assert used == set(live[table]), (table, used, live[table])  # documented column sources use exactly the pipeline's dependencies


def test_markdown_lists_every_table_and_column(lake):
    md = lineage.to_markdown()
    for table, (grain, cols) in lineage.GOLD_LINEAGE.items():
        assert f"## gold.{table}" in md
        assert all(f"| {c} |" in md for c in cols)
