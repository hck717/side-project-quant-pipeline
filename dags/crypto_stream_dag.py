import sys
sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.crypto_producer import run_crypto_tick_producer

default_args = {"owner": "ho", "retries": 0}

with DAG(
    dag_id="crypto_stream_dag",
    default_args=default_args,
    start_date=datetime(2025, 9, 17),
    schedule_interval="* * * * *",  # every minute
    catchup=False,
) as dag:
    stream_crypto = PythonOperator(
        task_id="stream_crypto_ticks",
        python_callable=run_crypto_tick_producer
    )
