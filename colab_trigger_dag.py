from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
import subprocess
import requests
import json
from confluent_kafka import Consumer

GITHUB_TOKEN = "<YOUR_GITHUB_PAT>"
REPO = "youruser/yourrepo"

def start_ngrok():
    # Start ngrok in background
    proc = subprocess.Popen(["ngrok", "tcp", "9092"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    import time; time.sleep(3)
    # Query ngrok API for tunnel info
    resp = requests.get("http://127.0.0.1:4040/api/tunnels")
    public_url = resp.json()['tunnels'][0]['public_url'].replace("tcp://", "")
    return public_url

def trigger_github_actions(ti):
    broker_uri = ti.xcom_pull(task_ids="start_ngrok")
    url = f"https://api.github.com/repos/{REPO}/actions/workflows/run_colab.yml/dispatches"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    data = {"ref": "main", "inputs": {"broker_uri": broker_uri}}
    r = requests.post(url, headers=headers, data=json.dumps(data))
    r.raise_for_status()
    print(f"Triggered GitHub Actions with broker {broker_uri}")

def check_kafka():
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'airflow-check',
        'auto.offset.reset': 'latest'
    })
    c.subscribe(['equities_ticks', 'crypto_ticks', 'bonds_data'])
    msg = c.poll(1.0)
    c.close()
    return msg is not None

with DAG(
    dag_id="colab_trigger_dag",
    start_date=datetime(2025, 9, 18),
    schedule_interval=None,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id="start_ngrok",
        python_callable=start_ngrok
    )
    t2 = PythonOperator(
        task_id="trigger_colab",
        python_callable=trigger_github_actions
    )
    t3 = PythonSensor(
        task_id="wait_for_kafka",
        python_callable=check_kafka,
        poke_interval=10,
        timeout=300
    )

    t1 >> t2 >> t3
