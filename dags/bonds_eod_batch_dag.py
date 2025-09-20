import sys
sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.bonds_eod_writer import run_bonds_eod_writer

default_args = {"owner": "ho", "retries": 0}

with DAG(
    dag_id="bonds_eod_batch_dag",
    default_args=default_args,
    start_date=datetime(2025, 9, 17),
    schedule_interval="0 22 * * *",  # daily at 10 PM
    catchup=False,
) as dag:
    batch_bonds_eod = PythonOperator(
        task_id="batch_bonds_eod",
        python_callable=run_bonds_eod_writer
    )
