"""Airflow DAG: raw -> silver (PySpark) -> Snowflake -> fact table, with validation and reconciliation."""
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from pipeline import lake, snowflake_ops

PROJECT = "/opt/airflow/project"
FILES = ["customers.csv", "policies.csv", "claims_part1.csv", "claims_part2.csv", "customers_changes.csv"]
log = logging.getLogger("claims_pipeline")


def alert_on_failure(context):
    """Called when a task fails after all retries. Replace with an email or Slack call in real life."""
    ti = context["task_instance"]
    log.error("ALERT: task %s in DAG %s failed (run %s)", ti.task_id, ti.dag_id, context["ds"])


@dag(
    dag_id="claims_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1), "on_failure_callback": alert_on_failure},
    tags=["insurance", "medallion"],
)
def claims_pipeline():
    @task
    def copy_to_raw(fname: str, ds=None) -> str:
        return lake.copy_file(f"{PROJECT}/data", f"{PROJECT}/lake/raw/{ds}", fname)

    @task
    def validate_raw(paths) -> None:
        lake.validate_files(list(paths))

    silver_clean = BashOperator(
        task_id="silver_clean",
        bash_command=f"python {PROJECT}/spark/silver_clean.py",
        env={"LAKE_RAW": PROJECT + "/lake/raw/{{ ds }}", "LAKE_SILVER": PROJECT + "/lake/silver/{{ ds }}"},
        append_env=True,
    )

    @task
    def load_snowflake(ds=None) -> None:
        conn = snowflake_ops.connect()
        try:
            snowflake_ops.ensure_objects(conn)
            n = snowflake_ops.upload_silver(conn, f"{PROJECT}/lake/silver/{ds}")
            log.info("uploaded %s silver file(s) to the stage", n)
            snowflake_ops.copy_into_raw(conn)
            snowflake_ops.merge_fact(conn, f"{PROJECT}/sql/04_merge_fact.sql")
        finally:
            conn.close()

    @task
    def reconcile(ds=None) -> dict:
        conn = snowflake_ops.connect()
        try:
            result = snowflake_ops.reconcile(conn, f"{PROJECT}/lake/silver/{ds}")
        finally:
            conn.close()
        log.info("reconciled: %s", result)
        return result

    landed = copy_to_raw.expand(fname=FILES)
    validated = validate_raw(landed)
    validated >> silver_clean >> load_snowflake() >> reconcile()


claims_pipeline()
