import sys
sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.bonds_eod_writer import run_bonds_eod_writer
from scripts.crypto_producer import run_crypto_tick_producer
from scripts.equities_eod_writer import run_equities_eod_writer
from scripts.equities_intraday_producer import run_equities_intraday_producer

default_args = {"owner": "ho", "retries": 0}

with DAG(
    dag_id="full_pipeline_test_dag",
    default_args=default_args,
    start_date=datetime(2025, 9, 17),
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:
    bonds_task = PythonOperator(
        task_id="batch_bonds_eod",
        python_callable=run_bonds_eod_writer
    )
    equities_eod_task = PythonOperator(
        task_id="batch_equities_eod",
        python_callable=run_equities_eod_writer
    )
    crypto_task = PythonOperator(
        task_id="stream_crypto_ticks",
        python_callable=run_crypto_tick_producer
    )
    equities_intraday_task = PythonOperator(
        task_id="stream_equities_intraday",
        python_callable=run_equities_intraday_producer
    )
    # Dependencies: Run all tasks in parallel
    [bonds_task, equities_eod_task, crypto_task, equities_intraday_task]