from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.equities_eod_writer import run_equities_eod_writer

default_args = {"owner": "ho", "retries": 0}

with DAG(
    dag_id="equities_eod_batch_dag",
    default_args=default_args,
    start_date=datetime(2025, 9, 17),
    schedule_interval="0 22 * * *",  # daily at 10 PM
    catchup=False,
) as dag:

    batch_equities_eod = PythonOperator(
        task_id="batch_equities_eod",
        python_callable=run_equities_eod_writer
    )
