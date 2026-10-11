from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.sensors.sql import SqlSensor

WIB = pendulum.timezone("Asia/Jakarta")
ACCOUNT_SNAPSHOT = Dataset("s3://negarabank-silver/account_snapshots")


def alert_on_failure(context):
    ti = context["ti"]
    SlackWebhookHook(slack_webhook_conn_id="slack_data_alerts").send(
        text=(f":rotating_light: {ti.dag_id}.{ti.task_id} failed after {ti.try_number} attempts "
              f"for {context['ds']}\nLog: {ti.log_url}")
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": alert_on_failure,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="silver_account_snapshot_daily",
    start_date=pendulum.datetime(2026, 1, 1, tz=WIB),
    schedule="30 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "ojk"],
) as producer:

    wait_for_eod = SqlSensor(
        task_id="wait_for_core_banking_eod",
        conn_id="oracle_core_banking_replica",
        sql="SELECT 1 FROM batch_control WHERE batch_name = 'EOD' "
            "AND business_date = TO_DATE('{{ ds }}', 'YYYY-MM-DD') AND status = 'DONE'",
        mode="reschedule", poke_interval=120, timeout=60 * 60 * 2,
        retries=0,
    )

    run_snapshot = DatabricksRunNowOperator(
        task_id="run_account_snapshot",
        databricks_conn_id="databricks_default",
        job_name="silver_account_snapshot",
        python_params=["--snapshot-date", "{{ ds }}", "--env", "prod"],
    )

    def _check_partition(**ctx):
        """Make sure the day's partition really has rows before telling consumers it is ready."""
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        rows = DatabricksSqlHook("databricks_sql").get_first(
            "SELECT count(*) FROM silver.account_snapshots WHERE snapshot_date = %(d)s",
            parameters={"d": ctx["ds"]})[0]
        if not rows:
            raise AirflowFailException(f"Partition {ctx['ds']} is empty")

    publish = PythonOperator(
        task_id="publish_snapshot_ready",
        python_callable=_check_partition,
        outlets=[ACCOUNT_SNAPSHOT], 
    )

    wait_for_eod >> run_snapshot >> publish


with DAG(
    dag_id="credit_scoring_daily",
    start_date=pendulum.datetime(2026, 1, 1, tz=WIB),
    schedule=[ACCOUNT_SNAPSHOT],
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "credit-scoring"],
) as consumer:

    DatabricksRunNowOperator(
        task_id="run_credit_scoring",
        databricks_conn_id="databricks_default",
        job_name="credit_scoring_batch_inference",
    )
