"""Unit tests for the account snapshot job (local Spark, no external dependencies)."""
import os
import sys
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import account_snapshot as job  # noqa: E402

D = date(2026, 2, 10)


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.session.timeZone", "Asia/Jakarta")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


def accounts_df(spark, balances):
    rows = [(a, "C1", t, "P", date(2024, 1, 1), "ACTIVE", Decimal(b),
             Decimal("1000") if t == "CREDIT_CARD" else None, Decimal("0.0100"))
            for a, t, b in balances]
    return spark.createDataFrame(
        rows, "account_id string, customer_id string, account_type string, product_name string, "
              "opened_date date, status string, balance decimal(15,2), credit_limit decimal(15,2), "
              "interest_rate decimal(5,4)")


def txns_df(spark, rows):
    return spark.createDataFrame(
        [(i, a, datetime(2026, 2, 10, 9, 0), t, Decimal(amt), "QRIS", s) for i, a, t, amt, s in rows],
        "txn_id string, account_id string, txn_date timestamp, txn_type string, amount decimal(15,2), "
        "channel string, status string")


def prev_df(spark, balances):
    return spark.createDataFrame([(a, Decimal(b)) for a, b in balances],
                                 "account_id string, closing_balance decimal(15,2)")


def cfg(tmp_path):
    return job.JobConfig(snapshot_date=D, env="local", output_path=str(tmp_path / "out"),
                         output_format="parquet")


def test_only_completed_txns_move_money_and_no_fanout(spark):
    acc = accounts_df(spark, [("A1", "SAVINGS", "150"), ("A2", "SAVINGS", "50")])
    tx = txns_df(spark, [("T1", "A1", "CREDIT", "100", "COMPLETED"),
                         ("T2", "A1", "DEBIT", "-50", "COMPLETED"),
                         ("T3", "A1", "DEBIT", "-999", "FAILED"),
                         ("T3", "A1", "DEBIT", "-999", "FAILED")])          # duplicate replay
    out = {r.account_id: r for r in job.build_snapshot(acc, tx, None, D, "r").collect()}
    assert len(out) == 2                                                     # one row per account
    assert out["A1"].net_amount == Decimal("50.00")
    assert out["A1"].total_credit_amount == Decimal("100.00")
    assert out["A1"].total_debit_amount == Decimal("50.00")
    assert out["A1"].txn_count == 3 and out["A1"].txn_count_failed == 1
    assert out["A2"].txn_count == 0 and out["A2"].net_amount == Decimal("0.00")  # zeros not NULLs


def test_dq_passes_when_balances_reconcile(spark, tmp_path):
    acc = accounts_df(spark, [("A1", "SAVINGS", "150")])
    tx = txns_df(spark, [("T1", "A1", "CREDIT", "50", "COMPLETED")])
    prev = prev_df(spark, [("A1", "100")])
    snap = job.build_snapshot(acc, tx, prev, D, "r")
    res = {c.name: c for c in job.run_quality_checks(snap, acc, tx, prev, cfg(tmp_path))}
    assert all(c.passed for c in res.values())


def test_dq_blocks_when_balance_does_not_reconcile(spark, tmp_path):
    acc = accounts_df(spark, [("A1", "SAVINGS", "999")])                    # should be 150
    tx = txns_df(spark, [("T1", "A1", "CREDIT", "50", "COMPLETED")])
    prev = prev_df(spark, [("A1", "100")])
    snap = job.build_snapshot(acc, tx, prev, D, "r")
    res = {c.name: c for c in job.run_quality_checks(snap, acc, tx, prev, cfg(tmp_path))}
    assert not res["balance_reconciliation"].passed and res["balance_reconciliation"].blocking


def test_orphan_transactions_are_detected(spark, tmp_path):
    acc = accounts_df(spark, [("A1", "SAVINGS", "0")])
    tx = txns_df(spark, [("T1", "UNKNOWN", "CREDIT", "50", "COMPLETED")])
    snap = job.build_snapshot(acc, tx, None, D, "r")
    res = {c.name: c for c in job.run_quality_checks(snap, acc, tx, None, cfg(tmp_path))}
    assert not res["txn_count_reconciles"].passed


class FakeSource:
    def __init__(self, acc, tx, prev=None):
        self.acc, self.tx, self.prev = acc, tx, prev

    def accounts(self): return self.acc
    def transactions(self): return self.tx
    def previous_snapshot(self): return self.prev


def test_rerun_is_idempotent_and_keeps_other_partitions(spark, tmp_path):
    c = cfg(tmp_path)
    acc = accounts_df(spark, [("A1", "SAVINGS", "150"), ("A2", "SAVINGS", "10")])
    tx = txns_df(spark, [("T1", "A1", "CREDIT", "50", "COMPLETED")])
    # an older partition that must survive
    other = job.JobConfig(snapshot_date=date(2026, 2, 9), env="local",
                          output_path=c.output_path, output_format="parquet")
    job.run(spark, other, FakeSource(acc, tx.limit(0)))
    job.run(spark, c, FakeSource(acc, tx))
    first = spark.read.parquet(c.output_path).filter(f"snapshot_date='{D}'").drop("_processed_at", "_run_id").collect()
    job.run(spark, c, FakeSource(acc, tx))                                  # re-run same date
    df = spark.read.parquet(c.output_path)
    second = df.filter(f"snapshot_date='{D}'").drop("_processed_at", "_run_id").collect()
    assert sorted(first) == sorted(second)
    assert df.filter("snapshot_date='2026-02-09'").count() == 2             # untouched
    assert df.count() == 4


def test_failed_dq_writes_nothing(spark, tmp_path):
    c = cfg(tmp_path)
    acc = accounts_df(spark, [("A1", "SAVINGS", "0")])
    tx = txns_df(spark, [("T1", "UNKNOWN", "CREDIT", "50", "COMPLETED")])
    with pytest.raises(job.DataQualityError):
        job.run(spark, c, FakeSource(acc, tx))
    assert not os.path.exists(c.output_path)
