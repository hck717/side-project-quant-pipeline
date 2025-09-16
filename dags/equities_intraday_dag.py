from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.equities_intraday_producer import run_equities_intraday_producer

DEFAULT_ARGS = dict(owner="ho", depends_on_past=False, retries=1, retry_delay=timedelta(seconds=30))

with DAG(
    dag_id="equities_intraday_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 9, 1),
    schedule_interval="*/5 * * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "equities"],
) as dag:
    PythonOperator(task_id="produce_equities_intraday_ticks", python_callable=run_equities_intraday_producer)
