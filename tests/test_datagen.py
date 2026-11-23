"""Generator v2: batch ids, CDC operations, source system, timestamps, configurable data quality."""
import pandas as pd
import pytest

from banklake import datagen
from banklake.schemas import ENTITIES


@pytest.fixture()
def raw(tmp_path, monkeypatch):
    monkeypatch.setattr(datagen, "RAW", tmp_path)
    return tmp_path


def _csv(raw, entity, name):
    return pd.read_csv(raw / entity / name, dtype=str)


def test_every_source_file_carries_traceability_columns(raw):
    datagen.generate_full("small")
    for e, spec in ENTITIES.items():
        f = next((raw / e).glob("*_full.*"))
        df = _csv(raw, e, f.name) if f.suffix == ".csv" else datagen.read_jsonl(f)
        for c in ("source_system", "batch_id", "op_type", "created_at", "updated_at"):
            assert c in df.columns, (e, c)
        assert set(df["batch_id"]) == {"1"} and set(df["op_type"]) == {"I"}


def test_second_batch_contains_real_changes(raw):
    datagen.generate_full("small")
    info = datagen.generate_incremental(deletes=True)
    assert info["batch"] == 2 and info["deleted_customers"] > 0 and info["deleted_transactions"] > 0
    inc = _csv(raw, "customer", "customer_inc_1.csv")
    assert set(inc["op_type"]) == {"I", "U", "D"} and set(inc["batch_id"]) == {"2"}
    full = _csv(raw, "customer", "customer_full.csv")
    upd = inc[inc.op_type == "U"]
    assert upd["customer_id"].isin(full["customer_id"]).all()          # updates hit existing records
    assert not inc[inc.op_type == "I"]["customer_id"].isin(full["customer_id"]).any()  # inserts are new
    deleted = inc[inc.op_type == "D"]["customer_id"]
    acc = _csv(raw, "account", "account_full.csv")
    assert not deleted.isin(acc["customer_id"]).any()                  # deletes never orphan an account
    changed = upd.merge(full.drop_duplicates("customer_id"), on="customer_id", suffixes=("_new", "_old"))
    assert (changed["updated_at_new"] > changed["updated_at_old"]).all()


def test_default_incremental_has_no_deletes(raw):
    datagen.generate_full("small")
    datagen.generate_incremental()
    inc = _csv(raw, "customer", "customer_inc_1.csv")
    assert "D" not in set(inc["op_type"])


def test_dq_rate_zero_gives_clean_data(raw):
    m = datagen.generate_full("small", dq_rate=0)
    assert all(v == 0 for ent in m.values() for v in ent.values())
    cust = _csv(raw, "customer", "customer_full.csv")
    assert cust["customer_name"].notna().all() and cust["customer_id"].is_unique
    txn = datagen.read_jsonl(raw / "transaction" / "transaction_full.json").astype({"amount": float})
    assert (txn["amount"] > 0).all()


def test_dq_rate_scales_anomalies(raw):
    lo = datagen.generate_full("small", dq_rate=1)["transaction"]["NEGATIVE_AMOUNT"]
    hi = datagen.generate_full("small", dq_rate=3)["transaction"]["NEGATIVE_AMOUNT"]
    assert hi > lo
