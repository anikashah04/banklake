"""Silver Transform handler. Input: the Silver Merge table. Output: valid rows in lake/silver/<entity>,
rejected rows in lake/quarantine/silver_<entity>.

Incremental: only merge rows newer than the watermark are processed. Their keys replace the old Silver rows;
keys the source soft-deleted are removed from Silver. A changed record that fails validation is quarantined and
the previous good version stays in Silver (last-known-good).
"""
import os
import shutil

from pyspark.sql import functions as F

from .. import meta, silver_merge, vault
from ..common import layer_path
from ..schemas import ENTITIES
from . import cleaning, rules


def _swap_write(df, path):
    p = str(path)
    tmp = p + "__tmp"
    df.write.mode("overwrite").parquet(tmp)
    shutil.rmtree(p)
    os.rename(tmp, p)


def _run(spark, m, ctx, entity):
    pid, key = m["pipeline_id"], ENTITIES[entity]["key"]
    strategy = "full" if ctx.full_refresh else m["load_strategy"]
    wmcol = m["watermark_column"] or "_batch_id"
    wm = meta.get_watermark(pid) if strategy == "incremental" else -1
    changed = silver_merge.read(spark, entity).filter(F.col(wmcol) > wm).cache()
    rows_in = changed.count()
    if rows_in == 0:
        return dict(rows_in=0, rows_out=0, rows_rejected=0)

    deleted_keys = changed.filter("_is_deleted").select(key).distinct()
    live = changed.filter(~F.col("_is_deleted"))
    df = rules.RULES[entity](spark, cleaning.CLEANERS[entity](live))     # clean -> validate (still clear text)
    df = vault.protect(df, entity, actor="silver_transform")              # mask + tokenise; real values -> vault
    good = df.filter("reject_reason IS NULL").drop("reject_reason").cache()
    bad = df.filter("reject_reason IS NOT NULL").withColumn("_run_id", F.lit(ctx.run_id)).cache()
    n_good, n_bad = good.count(), bad.count()

    target, quarantine = layer_path("silver", entity), layer_path("quarantine", f"silver_{entity}")
    incremental = strategy == "incremental" and target.exists()
    n_removed = 0
    if incremental:
        existing = spark.read.parquet(str(target))
        n_removed = existing.join(deleted_keys, key, "left_semi").count()
        replace = good.select(key).union(deleted_keys).distinct()
        _swap_write(existing.join(replace, key, "left_anti").unionByName(good, allowMissingColumns=True), target)
        spark.catalog.refreshByPath(str(target))
        bad.write.mode("append").parquet(str(quarantine))
    else:
        good.write.mode("overwrite").parquet(str(target))
        bad.write.mode("overwrite").parquet(str(quarantine))
    batch = changed.agg(F.max(wmcol)).first()[0]
    meta.set_watermark(pid, batch, ctx.run_id)
    meta.execute(
        "INSERT INTO transformation_audit_log(run_id,pipeline_id,layer,entity,batch_id,load_type,rows_in,rows_out,"
        "deletes,rejected,ts) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (ctx.run_id, pid, "silver_transform", entity, batch, strategy.upper(), rows_in, n_good, n_removed, n_bad, meta.now()))
    return dict(rows_in=rows_in, rows_out=n_good, rows_rejected=n_bad)


def run_customer(spark, m, ctx): return _run(spark, m, ctx, "customer")  # noqa: E704
def run_account(spark, m, ctx): return _run(spark, m, ctx, "account")  # noqa: E704
def run_transaction(spark, m, ctx): return _run(spark, m, ctx, "transaction")  # noqa: E704
def run_branch(spark, m, ctx): return _run(spark, m, ctx, "branch")  # noqa: E704
def run_deposit(spark, m, ctx): return _run(spark, m, ctx, "deposit")  # noqa: E704
def run_loan(spark, m, ctx): return _run(spark, m, ctx, "loan")  # noqa: E704
