from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.bonds_eod_writer import run_bonds_eod_writer

DEFAULT_ARGS = dict(owner="ho", depends_on_past=False, retries=1, retry_delay=timedelta(minutes=2))

with DAG(
    dag_id="bonds_eod_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 23 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["batch", "bonds"],
) as dag:
    PythonOperator(task_id="write_bonds_parquet_to_minio", python_callable=run_bonds_eod_writer)
