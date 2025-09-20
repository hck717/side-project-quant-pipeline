from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
import subprocess
import requests
import json
import time
import logging
from confluent_kafka import Consumer


GITHUB_TOKEN = "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"
REPO = "hck717/side-project-quant-pipeline"

def get_ngrok_tcp():
    logging.info("🔍 Querying ngrok API for public TCP address...")
    resp = requests.get("http://ngrok:4040/api/tunnels")
    resp.raise_for_status()
    tunnels = resp.json().get("tunnels", [])
    tcp_tunnels = [t for t in tunnels if t["proto"] == "tcp"]
    if not tcp_tunnels:
        raise RuntimeError("No TCP tunnels found in ngrok API response")
    public_url = tcp_tunnels[0]['public_url'].replace("tcp://", "")
    logging.info(f"✅ Found ngrok public address: {public_url}")
    return public_url

def trigger_github_actions(ti):
    broker_uri = ti.xcom_pull(task_ids="get_ngrok_tcp")
    url = f"https://api.github.com/repos/{REPO}/actions/workflows/run_colab.yml/dispatches"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    data = {"ref": "main", "inputs": {"broker_uri": broker_uri}}
    r = requests.post(url, headers=headers, json=data)  # Use json= instead of data=
    r.raise_for_status()
    logging.info(f"Triggered GitHub Actions with broker {broker_uri}")

def check_kafka():
    c = Consumer({
        'bootstrap.servers': 'redpanda:9092',  # Use Docker service name
        'group.id': 'airflow-check',
        'auto.offset.reset': 'latest'
    })
    c.subscribe(['equities_ticks', 'crypto_ticks', 'bonds_data'])
    msg = c.poll(5.0)  # Increase poll timeout
    c.close()
    return msg is not None

with DAG(
    dag_id="colab_trigger_dag",
    start_date=datetime(2025, 9, 18),
    schedule_interval=None,
    catchup=False,
    default_args={
        'owner': 'quant',
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }
) as dag:
    t1 = PythonOperator(
        task_id="get_ngrok_tcp",
        python_callable=get_ngrok_tcp
    )
    t2 = PythonOperator(
        task_id="trigger_colab",
        python_callable=trigger_github_actions
    )
    t3 = PythonSensor(
        task_id="wait_for_kafka",
        python_callable=check_kafka,
        poke_interval=30,
        timeout=600
    )

    t1 >> t2 >> t3