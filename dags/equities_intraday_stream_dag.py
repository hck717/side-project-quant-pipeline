import sys
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.equities_intraday_producer import run_equities_intraday_producer

default_args = {"owner": "ho", "retries": 0}

with DAG(
    dag_id="equities_intraday_stream_dag",
    default_args=default_args,
    start_date=datetime(2025, 9, 17),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
) as dag:

    stream_equities_intraday = PythonOperator(
        task_id="stream_equities_intraday",
        python_callable=run_equities_intraday_producer
    )
