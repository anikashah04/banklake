"""Pure Spark logic for change data capture: key validation, latest-record selection, and classifying
each incoming change against the current target as INSERT / UPDATE / DELETE / ignored."""
from pyspark.sql import Window
from pyspark.sql import functions as F

VALID_OPS = ("I", "U", "D")


def validate_and_dedupe(df, key: str):
    """Split incoming rows into (latest_per_key, rejected, n_duplicates_removed).

    Rejected: blank primary key or an unknown op_type. Latest record = newest updated_at, then newest
    source batch, then newest ingestion - so a late duplicate never beats a newer version.
    """
    op = F.upper(F.trim(F.coalesce(F.col("op_type"), F.lit("I"))))
    d = df.withColumn("op_type", op)
    d = d.withColumn("_reject", F.when(F.col(key).isNull() | (F.trim(F.col(key)) == ""), "NULL_PRIMARY_KEY")
                     .when(~F.col("op_type").isin(*VALID_OPS), "INVALID_OP_TYPE"))
    bad = d.filter("_reject IS NOT NULL").withColumnRenamed("_reject", "reject_reason")
    ok = d.filter("_reject IS NULL").drop("_reject")
    w = Window.partitionBy(key).orderBy(
        F.to_timestamp("updated_at").desc_nulls_last(), F.col("batch_id").cast("int").desc_nulls_last(),
        F.col("_batch_id").desc(), F.col("_ingest_ts").desc())
    latest = ok.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return latest, bad


def classify(latest, existing, key: str):
    """Label each latest change with _merge_action by comparing with the target (None = full load).

    INSERT        new key
    UPDATE        existing key, incoming is not older (includes re-activating a deleted key)
    DELETE        op D on an existing key -> soft delete
    STALE         incoming change is older than what the target already holds -> ignored
    DELETE_NO_TARGET  delete for a key the target has never seen -> ignored
    """
    if existing is None:
        return latest.withColumn("_merge_action", F.lit("INSERT"))
    e = existing.select(F.col(key).alias("_e_key"), F.to_timestamp("updated_at").alias("_e_ts"))
    j = latest.join(F.broadcast(e), latest[key] == F.col("_e_key"), "left")
    new_ts, found = F.to_timestamp("updated_at"), F.col("_e_key").isNotNull()
    stale = found & F.col("_e_ts").isNotNull() & new_ts.isNotNull() & (new_ts < F.col("_e_ts"))
    action = (F.when(stale, "STALE")
              .when(F.col("op_type") == "D", F.when(found, "DELETE").otherwise("DELETE_NO_TARGET"))
              .when(found, "UPDATE").otherwise("INSERT"))
    return j.withColumn("_merge_action", action).drop("_e_key", "_e_ts")


def to_target_rows(classified, run_id: str):
    """Rows that are actually written: everything except ignored changes, with soft-delete columns."""
    rows = classified.filter(~F.col("_merge_action").isin("STALE", "DELETE_NO_TARGET"))
    is_del = F.col("op_type") == "D"
    return (rows.withColumn("_is_deleted", is_del)
            .withColumn("_deleted_ts", F.when(is_del, F.to_timestamp("updated_at")))
            .withColumn("_merge_run_id", F.lit(run_id))
            .withColumn("_merge_ts", F.current_timestamp()))
