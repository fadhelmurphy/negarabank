"""Local version of the snapshot to credit scoring flow, for docker-compose.

Same shape as q1/dags/account_snapshot_dag.py, but the Databricks job is a call to the PySpark script and
Slack is replaced by a line in logs/alerts.log. The Oracle sensor is left out (static sample data).
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator

TZ = pendulum.timezone("Asia/Jakarta")
OUTPUT = "/opt/airflow/silver/account_snapshots"
SNAPSHOTS = Dataset("file:///opt/airflow/silver/account_snapshots")


def snapshot_date(context) -> str:
    """Tanggal yang diproses: dari parameter DAG (default 2026-02-13, tanggal di data sample)."""
    return context["params"].get("snapshot_date") or context["ds"]


def alert_on_final_failure(context):
    ti = context["ti"]
    msg = (f"{datetime.now():%F %T} ALERT {ti.dag_id}.{ti.task_id} date={snapshot_date(context)} "
           f"failed after {ti.try_number} attempt(s): {str(context.get('exception'))[:200]}")
    print(msg)
    with open("/opt/airflow/logs/alerts.log", "a") as f:   # stands in for Slack
        f.write(msg + "\n")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "on_failure_callback": alert_on_final_failure,   # only after the last attempt
    "execution_timeout": timedelta(hours=1),
}


def run_snapshot(**context):
    d = snapshot_date(context)
    print(f"snapshot_date = {d}")
    r = subprocess.run(
        ["python", "/opt/airflow/project/q1/account_snapshot.py", "--snapshot-date", d,
         "--env", "local", "--local-source", "/opt/airflow/data", "--output", OUTPUT],
        capture_output=True, text=True)
    print(r.stdout[-4000:])
    if r.returncode == 2:
        raise AirflowFailException("Data-quality gate failed: retrying cannot fix bad data")
    if r.returncode != 0:
        raise AirflowException(f"snapshot job failed (exit {r.returncode}): {r.stderr[-1500:]}")


def verify_partition(**context):
    path = f"{OUTPUT}/snapshot_date={snapshot_date(context)}"
    if not os.path.isdir(path) or not os.listdir(path):
        raise AirflowFailException(f"Partition {path} missing/empty after a 'successful' run")


with DAG(
    dag_id="silver_account_snapshot_daily_local",
    start_date=pendulum.datetime(2026, 1, 1, tz=TZ),
    schedule=None,
    params={"snapshot_date": Param("2026-02-13", type="string",
                                   description="Tanggal yang diproses (data sample: 2025-08-01 s/d 2026-02-14)")},
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["negarabank", "q1"],
) as producer:
    snapshot = PythonOperator(task_id="run_account_snapshot", python_callable=run_snapshot)
    publish = PythonOperator(task_id="publish_snapshot_ready", python_callable=verify_partition,
                             outlets=[SNAPSHOTS])
    snapshot >> publish

with DAG(
    dag_id="credit_scoring_daily_local",
    start_date=pendulum.datetime(2026, 1, 1, tz=TZ),
    schedule=[SNAPSHOTS],
    catchup=False,
    default_args=default_args,
    tags=["negarabank", "q1"],
) as consumer:
    PythonOperator(
        task_id="run_credit_scoring",
        python_callable=lambda **_: print(
            "credit scoring would start now (placeholder for the Databricks ML job)"))
