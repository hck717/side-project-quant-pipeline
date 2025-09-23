from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "ho",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mongo_consolidate_dag",
    default_args=default_args,
    description="Consolidate MongoDB data",
    schedule_interval="0 2 * * *",  # 2:00 AM daily (after batch processing)
    start_date=datetime(2025, 9, 18),
    catchup=False,
) as dag:
    
    consolidate_data = BashOperator(
        task_id="consolidate_mongodb_data",
        bash_command="cd /opt/airflow/scripts/processing && python mongo_consolidate.py",
    )