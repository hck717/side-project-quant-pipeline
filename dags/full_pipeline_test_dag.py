from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import the updated functions from your scripts
from scripts.crypto_producer import run_crypto_tick_producer
from scripts.equities_intraday_producer import run_equities_intraday_producer
from scripts.equities_eod_writer import run_equities_eod_writer
from scripts.bonds_eod_writer import run_bonds_eod_writer

default_args = {
    "owner": "ho",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="full_pipeline_test_dag",
    default_args=default_args,
    description="Run all four data pipeline scripts in sequence for testing",
    start_date=datetime(2025, 9, 16),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["test", "pipeline"],
) as dag:

    crypto_task = PythonOperator(
        task_id="crypto_producer_task",
        python_callable=run_crypto_tick_producer
    )

    equities_intraday_task = PythonOperator(
        task_id="equities_intraday_producer_task",
        python_callable=run_equities_intraday_producer
    )

    equities_eod_task = PythonOperator(
        task_id="equities_eod_writer_task",
        python_callable=run_equities_eod_writer
    )

    bonds_eod_task = PythonOperator(
        task_id="bonds_eod_writer_task",
        python_callable=run_bonds_eod_writer
    )

    # Run in sequence so you can see each stage clearly in logs
    crypto_task >> equities_intraday_task >> equities_eod_task >> bonds_eod_task
