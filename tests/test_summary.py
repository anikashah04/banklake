"""The shared summary reconciles by construction and agrees with the real lake."""
from banklake import summary
from banklake.schemas import ORDER


def test_each_stage_input_is_the_previous_stage_output_and_every_row_is_accounted_for(lake):
    from banklake import orchestrator
    orchestrator.run(retry_sleep=0)   # earlier tests may stop after Silver Merge; bring every stage up to date first
    ds = summary.datasets()
    assert [d["dataset"] for d in ds] == list(ORDER)
    for d in ds:
        assert d["merge_in"] == d["bronze"] and d["transform_in"] == d["merge_out"], d["dataset"]
        assert d["merge_in"] == d["merge_out"] + d["merge_rejected"] + d["merge_superseded"], d["dataset"]
        assert d["transform_in"] == d["transform_out"] - d["transform_kept_previous"] + d["transform_rejected"] + d["transform_deleted"] + d["transform_unexplained"]
        assert d["transform_unexplained"] == 0, d   # a clean run leaves nothing unexplained
        assert d["merge_superseded"] >= 0


def test_totals_are_the_sum_of_the_datasets(lake):
    ds, t = summary.datasets(), summary.stage_totals()
    for key in ("bronze", "merge_out", "transform_out", "transform_rejected", "bronze_rejected"):
        assert t[key] == sum(d[key] for d in ds), key
    assert t["rejected_total"] == t["bronze_rejected"] + t["merge_rejected"] + t["transform_rejected"]


def test_gold_tables_report_real_rows(lake):
    g = summary.gold_tables()
    assert len(g) == 5 and all(x["rows"] and x["rows"] > 0 for x in g)


def test_source_records_equal_accepted_plus_rejected_in_bronze(lake):
    for d in summary.datasets():
        assert d["source"] == d["bronze"] + d["bronze_rejected"], d
