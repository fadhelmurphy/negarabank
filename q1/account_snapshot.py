"""Daily account snapshot (silver).

Reads one business day of transactions from Oracle, joins them to the accounts table, runs the quality
checks and writes one partition per snapshot_date. Re-running a date replaces only that date's partition.

    spark-submit account_snapshot.py --snapshot-date 2026-02-14 --env prod
    python account_snapshot.py --snapshot-date 2026-02-14 --env local --local-source ../data --output /tmp/silver
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOG = logging.getLogger("account_snapshot")

BUSINESS_TZ = "Asia/Jakarta"  # business day is the WIB calendar day, not the cluster (UTC) day

ACCOUNT_COLUMNS = [
    "account_id", "customer_id", "account_type", "product_name", "opened_date",
    "status", "balance", "credit_limit", "interest_rate",
]
TXN_COLUMNS = ["txn_id", "account_id", "txn_date", "txn_type", "amount", "channel", "status"]

SNAPSHOT_SCHEMA = T.StructType([
    T.StructField("snapshot_date", T.DateType(), False),
    T.StructField("account_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("account_type", T.StringType(), True),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("account_status", T.StringType(), True),
    T.StructField("opened_date", T.DateType(), True),
    T.StructField("opening_balance", T.DecimalType(17, 2), True),
    T.StructField("closing_balance", T.DecimalType(15, 2), True),
    T.StructField("credit_limit", T.DecimalType(15, 2), True),
    T.StructField("interest_rate", T.DecimalType(5, 4), True),
    T.StructField("txn_count", T.LongType(), False),
    T.StructField("txn_count_completed", T.LongType(), False),
    T.StructField("txn_count_failed", T.LongType(), False),
    T.StructField("total_credit_amount", T.DecimalType(25, 2), False),
    T.StructField("total_debit_amount", T.DecimalType(25, 2), False),
    T.StructField("net_amount", T.DecimalType(25, 2), False),
    T.StructField("last_txn_ts", T.TimestampType(), True),
    T.StructField("_run_id", T.StringType(), False),
    T.StructField("_source_system", T.StringType(), False),
    T.StructField("_processed_at", T.TimestampType(), False),
])


# Configuration & secrets
@dataclass(frozen=True)
class JobConfig:
    snapshot_date: date
    env: str
    output_path: str
    output_format: str = "delta"              # delta in prod, parquet for local tests
    jdbc_url: Optional[str] = None
    secret_scope: str = "negarabank-core-banking"
    jdbc_fetch_size: int = 10_000              # Oracle default is only 10 rows per round trip
    jdbc_num_partitions: int = 16
    local_source: Optional[str] = None
    # DQ thresholds
    max_row_count_change_pct: float = 5.0      # vs previous snapshot
    reconciliation_tolerance: float = 1.00     # Rp
    reconciliation_min_pass_rate: float = 0.999

    @property
    def window_start(self) -> datetime:
        return datetime.combine(self.snapshot_date, datetime.min.time())

    @property
    def window_end(self) -> datetime:
        return self.window_start + timedelta(days=1)


def get_jdbc_credentials(cfg: JobConfig) -> Dict[str, str]:
    """Databricks secret scope first, then AWS Secrets Manager, env vars only for local/dev."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(SparkSession.getActiveSession())
        return {
            "user": dbutils.secrets.get(cfg.secret_scope, "oracle-etl-user"),
            "password": dbutils.secrets.get(cfg.secret_scope, "oracle-etl-password"),
        }
    except ImportError:
        pass

    secret_id = os.getenv("CORE_BANKING_SECRET_ID")
    if secret_id:
        import boto3
        payload = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        secret = json.loads(payload["SecretString"])
        return {"user": secret["username"], "password": secret["password"]}

    if cfg.env in ("local", "dev"):
        return {"user": os.environ["ORACLE_USER"], "password": os.environ["ORACLE_PASSWORD"]}
    raise RuntimeError("No secret backend available for env=%s" % cfg.env)


# Extract
class SourceReader:
    """JDBC in prod, CSV when running locally."""

    def __init__(self, spark: SparkSession, cfg: JobConfig):
        self.spark, self.cfg = spark, cfg

    def _jdbc(self, query: str, partition_col: Optional[str] = None) -> DataFrame:
        creds = get_jdbc_credentials(self.cfg)
        reader = (
            self.spark.read.format("jdbc")
            .option("url", self.cfg.jdbc_url)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("query" if partition_col is None else "dbtable", query if partition_col is None else f"({query}) q")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .option("fetchsize", self.cfg.jdbc_fetch_size)
            .option("sessionInitStatement", "ALTER SESSION SET TIME_ZONE = 'Asia/Jakarta'")
        )
        if partition_col:
            # parallel read instead of one cursor
            reader = (reader.option("partitionColumn", partition_col)
                            .option("lowerBound", 0)
                            .option("upperBound", self.cfg.jdbc_num_partitions)
                            .option("numPartitions", self.cfg.jdbc_num_partitions))
        return reader.load()

    def accounts(self) -> DataFrame:
        if self.cfg.local_source:
            return self._csv("accounts", ACCOUNT_COLUMNS)
        n = self.cfg.jdbc_num_partitions
        # Read from the replica after the end-of-day batch, so balances are final for the day.
        q = (f"SELECT {', '.join(ACCOUNT_COLUMNS)}, MOD(ORA_HASH(account_id), {n}) AS part_key "
             f"FROM ACCOUNTS")
        return self._jdbc(q, "part_key").drop("part_key")

    def transactions(self) -> DataFrame:
        if self.cfg.local_source:
            df = self._csv("transactions", TXN_COLUMNS)
            return df.filter((F.col("txn_date") >= F.lit(self.cfg.window_start)) &
                             (F.col("txn_date") < F.lit(self.cfg.window_end)))
        n = self.cfg.jdbc_num_partitions
        start = self.cfg.window_start.strftime("%Y-%m-%d %H:%M:%S")
        end = self.cfg.window_end.strftime("%Y-%m-%d %H:%M:%S")
        # the date filter runs inside Oracle so only one day is transferred
        q = (f"SELECT {', '.join(TXN_COLUMNS)}, MOD(ORA_HASH(txn_id), {n}) AS part_key "
             f"FROM TRANSACTIONS "
             f"WHERE txn_date >= TO_TIMESTAMP('{start}', 'YYYY-MM-DD HH24:MI:SS') "
             f"AND txn_date < TO_TIMESTAMP('{end}', 'YYYY-MM-DD HH24:MI:SS')")
        return self._jdbc(q, "part_key").drop("part_key")

    def previous_snapshot(self) -> Optional[DataFrame]:
        prev = self.cfg.snapshot_date - timedelta(days=1)
        try:
            df = self.spark.read.format(self.cfg.output_format).load(self.cfg.output_path)
            df = df.filter(F.col("snapshot_date") == F.lit(prev))
            return df if df.limit(1).count() > 0 else None
        except Exception:  # first run, table does not exist yet
            LOG.warning("No previous snapshot found for %s (first run?)", prev)
            return None

    def _csv(self, name: str, cols: List[str]) -> DataFrame:
        path = os.path.join(self.cfg.local_source, f"{name}.csv")
        return (self.spark.read.option("header", True).option("inferSchema", True)
                .csv(path).select(*cols))


# Transform (pure function -> unit-testable)
def build_snapshot(
    accounts: DataFrame,
    txns: DataFrame,
    prev_snapshot: Optional[DataFrame],
    snapshot_date: date,
    run_id: str,
) -> DataFrame:
    """One row per account per day. Transactions are aggregated before the join, only COMPLETED
    ones move money, and accounts with no activity get zeros instead of NULLs."""
    amount = F.col("amount").cast(T.DecimalType(15, 2))
    completed = F.col("status") == "COMPLETED"

    txn_agg = (
        txns.dropDuplicates(["txn_id"])  # the source can replay rows
        .groupBy("account_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum(completed.cast("long")).alias("txn_count_completed"),
            F.sum((F.col("status") == "FAILED").cast("long")).alias("txn_count_failed"),
            F.sum(F.when(completed & (amount > 0), amount).otherwise(F.lit(0))).alias("total_credit_amount"),
            F.sum(F.when(completed & (amount < 0), -amount).otherwise(F.lit(0))).alias("total_debit_amount"),
            F.sum(F.when(completed, amount).otherwise(F.lit(0))).alias("net_amount"),
            F.max("txn_date").alias("last_txn_ts"),
        )
    )

    acct = accounts.select(
        "account_id", "customer_id", "account_type", "product_name",
        F.col("status").alias("account_status"),
        F.col("opened_date").cast("date").alias("opened_date"),
        F.col("balance").cast(T.DecimalType(15, 2)).alias("closing_balance"),
        F.col("credit_limit").cast(T.DecimalType(15, 2)).alias("credit_limit"),
        F.col("interest_rate").cast(T.DecimalType(5, 4)).alias("interest_rate"),
    ).filter(F.col("opened_date") <= F.lit(snapshot_date))

    # left join so accounts without transactions today still get a row
    snap = acct.join(txn_agg, "account_id", "left")

    if prev_snapshot is not None:
        prev = prev_snapshot.select("account_id", F.col("closing_balance").alias("opening_balance"))
        snap = snap.join(prev, "account_id", "left")
    else:
        snap = snap.withColumn("opening_balance", F.lit(None).cast(T.DecimalType(17, 2)))

    zero = F.lit(0)
    snap = (
        snap.withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .withColumn("txn_count", F.coalesce("txn_count", zero).cast("long"))
        .withColumn("txn_count_completed", F.coalesce("txn_count_completed", zero).cast("long"))
        .withColumn("txn_count_failed", F.coalesce("txn_count_failed", zero).cast("long"))
        .withColumn("total_credit_amount", F.coalesce("total_credit_amount", zero).cast(T.DecimalType(25, 2)))
        .withColumn("total_debit_amount", F.coalesce("total_debit_amount", zero).cast(T.DecimalType(25, 2)))
        .withColumn("net_amount", F.coalesce("net_amount", zero).cast(T.DecimalType(25, 2)))
        .withColumn("opening_balance", F.col("opening_balance").cast(T.DecimalType(17, 2)))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_system", F.lit("oracle.core_banking"))
        .withColumn("_processed_at", F.current_timestamp())
    )
    return snap.select(*[f.name for f in SNAPSHOT_SCHEMA.fields])


# Data quality
@dataclass
class CheckResult:
    name: str
    passed: bool
    blocking: bool
    details: Dict = field(default_factory=dict)


class DataQualityError(Exception):
    pass


def run_quality_checks(
    snap: DataFrame,
    source_accounts: DataFrame,
    source_txns: DataFrame,
    prev_snapshot: Optional[DataFrame],
    cfg: JobConfig,
) -> List[CheckResult]:
    results: List[CheckResult] = []

    stats = snap.agg(
        F.count("*").alias("rows"),
        F.countDistinct("account_id").alias("distinct_accounts"),
        F.sum(F.col("account_id").isNull().cast("int") + F.col("customer_id").isNull().cast("int")).alias("null_keys"),
        F.sum("txn_count").alias("txn_count"),
        F.sum(((F.col("account_type") == "CREDIT_CARD") & F.col("credit_limit").isNull()).cast("int")).alias("cc_no_limit"),
    ).first()

    # one row per account, no null keys
    results.append(CheckResult("pk_unique_not_null", stats.rows == stats.distinct_accounts and stats.null_keys == 0, True,
                               {"rows": stats.rows, "distinct": stats.distinct_accounts, "null_keys": stats.null_keys}))

    # every eligible account is present
    expected = source_accounts.filter(F.col("opened_date") <= F.lit(cfg.snapshot_date)).count()
    results.append(CheckResult("row_count_matches_source", stats.rows == expected, True,
                               {"snapshot_rows": stats.rows, "source_rows": expected}))

    # no transactions lost in the join (orphans mean an FK problem upstream)
    src_txn = source_txns.select("txn_id").distinct().count()
    results.append(CheckResult("txn_count_reconciles", stats.txn_count == src_txn, True,
                               {"snapshot_txns": stats.txn_count, "source_txns": src_txn}))

    # row count should not jump from yesterday
    if prev_snapshot is not None:
        prev_rows = prev_snapshot.count()
        change = abs(stats.rows - prev_rows) / max(prev_rows, 1) * 100
        results.append(CheckResult("row_count_day_over_day", change <= cfg.max_row_count_change_pct, True,
                                   {"prev_rows": prev_rows, "rows": stats.rows, "change_pct": round(change, 3)}))

        # opening + net of completed transactions should equal closing
        recon = snap.filter(F.col("opening_balance").isNotNull()).select(
            (F.abs(F.col("opening_balance") + F.col("net_amount") - F.col("closing_balance"))
             <= F.lit(cfg.reconciliation_tolerance)).alias("ok"))
        r = recon.agg(F.count("*").alias("n"), F.sum(F.col("ok").cast("int")).alias("ok")).first()
        rate = (r.ok or 0) / r.n if r.n else 1.0
        results.append(CheckResult("balance_reconciliation", rate >= cfg.reconciliation_min_pass_rate, True,
                                   {"accounts_checked": r.n, "pass_rate": round(rate, 6),
                                    "threshold": cfg.reconciliation_min_pass_rate}))
    else:
        results.append(CheckResult("balance_reconciliation", True, False, {"skipped": "no previous snapshot"}))

    # credit cards should have a limit; reported only, does not stop the run
    results.append(CheckResult("credit_card_has_limit", stats.cc_no_limit == 0, False,
                               {"violations": stats.cc_no_limit}))

    for r in results:
        LOG.info("DQ %-28s %s blocking=%s %s", r.name, "PASS" if r.passed else "FAIL", r.blocking, r.details)
    return results


# Load (idempotent)
def write_snapshot(df: DataFrame, cfg: JobConfig) -> None:
    writer = df.repartition(8).sortWithinPartitions("account_id").write  # avoid lots of tiny files
    if cfg.output_format == "delta":
        # replaces one partition atomically
        (writer.format("delta").mode("overwrite")
         .option("replaceWhere", f"snapshot_date = '{cfg.snapshot_date.isoformat()}'")
         .partitionBy("snapshot_date")
         .save(cfg.output_path))
    else:
        df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        writer.mode("overwrite").partitionBy("snapshot_date").parquet(cfg.output_path)


# Orchestration of one run
def run(spark: SparkSession, cfg: JobConfig, source: Optional[SourceReader] = None) -> Dict:
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or str(uuid.uuid4())
    source = source or SourceReader(spark, cfg)
    t0 = time.time()
    LOG.info("Starting snapshot run_id=%s date=%s env=%s", run_id, cfg.snapshot_date, cfg.env)

    accounts = source.accounts().cache()
    txns = source.transactions().cache()
    prev = source.previous_snapshot()

    snap = build_snapshot(accounts, txns, prev, cfg.snapshot_date, run_id).cache()
    checks = run_quality_checks(snap, accounts, txns, prev, cfg)

    failed_blocking = [c for c in checks if c.blocking and not c.passed]
    metrics = {
        "run_id": run_id, "snapshot_date": cfg.snapshot_date.isoformat(),
        "rows": snap.count(), "duration_s": None,
        "checks": [asdict(c) for c in checks], "status": "FAILED_DQ" if failed_blocking else "SUCCESS",
    }
    if failed_blocking:
        metrics["duration_s"] = round(time.time() - t0, 1)
        LOG.error("Blocking DQ failures, NOT publishing: %s", [c.name for c in failed_blocking])
        raise DataQualityError(json.dumps(metrics, default=str))

    write_snapshot(snap, cfg)
    metrics["duration_s"] = round(time.time() - t0, 1)
    LOG.info("RUN_METRICS %s", json.dumps(metrics, default=str))
    for df in (accounts, txns, snap):
        df.unpersist()
    return metrics


def parse_args(argv=None) -> JobConfig:
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot-date", required=True, type=date.fromisoformat)
    p.add_argument("--env", default="prod", choices=["local", "dev", "staging", "prod"])
    p.add_argument("--output", default="s3://negarabank-silver/account_snapshots/")
    p.add_argument("--format", default=None, choices=["delta", "parquet"])
    p.add_argument("--jdbc-url", default=os.getenv("CORE_BANKING_JDBC_URL"))
    p.add_argument("--local-source", default=None)
    a = p.parse_args(argv)
    fmt = a.format or ("parquet" if a.env == "local" else "delta")
    return JobConfig(snapshot_date=a.snapshot_date, env=a.env, output_path=a.output,
                     output_format=fmt, jdbc_url=a.jdbc_url, local_source=a.local_source)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = parse_args(argv)
    spark = (SparkSession.builder.appName(f"account_snapshot_{cfg.snapshot_date}")
             .config("spark.sql.session.timeZone", BUSINESS_TZ)
             .config("spark.sql.shuffle.partitions", "64")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    try:
        run(spark, cfg)
        return 0
    except DataQualityError:
        return 2          # data problem, retrying will not help
    except Exception:
        LOG.exception("Snapshot run failed")
        return 1          # probably transient, the orchestrator can retry


if __name__ == "__main__":
    sys.exit(main())
