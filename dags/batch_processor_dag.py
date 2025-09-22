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
    dag_id="batch_processor_dag",
    default_args=default_args,
    description="Run batch processor for EOD data",
    schedule_interval="0 1 * * *",  # 1:00 AM daily (after EOD data is collected)
    start_date=datetime(2025, 9, 18),
    catchup=False,
) as dag:
    
    process_equities = BashOperator(
        task_id="process_equities_eod",
        bash_command="cd /opt/airflow/scripts/processing && python batch_processor.py --asset-type equities",
    )
    
    process_bonds = BashOperator(
        task_id="process_bonds_eod",
        bash_command="cd /opt/airflow/scripts/processing && python batch_processor.py --asset-type bonds",
    )
    
    process_equities >> process_bonds