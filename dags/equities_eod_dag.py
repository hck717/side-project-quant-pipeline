from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.equities_eod_writer import run_equities_eod_writer

DEFAULT_ARGS = dict(owner="ho", depends_on_past=False, retries=1, retry_delay=timedelta(minutes=2))

with DAG(
    dag_id="equities_eod_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 22 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["batch", "equities"],
) as dag:
    PythonOperator(task_id="write_eod_parquet_to_minio", python_callable=run_equities_eod_writer)
