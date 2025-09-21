# side-project-quant-pipeline
Local installs on macOS
You only need these on your Mac:

Docker Desktop → runs all containers.

VS Code → your IDE.

VS Code extensions:

Docker

Dev Containers

Python

YAML

Git → to manage your repo.

## Data Scope (Sprint 0)

**Crypto (real-time)**  
BTC-USD, ETH-USD, SOL-USD, ADA-USD, XRP-USD

**Equities (intraday, 5-min)**  
AAPL, MSFT, AMZN, TSLA, NVDA

**Equities (EOD)**  
AAPL, MSFT, AMZN, TSLA, NVDA, JPM, XOM, META, GOOGL, NFLX

**Bonds (EOD)**  
^TNX, ^IRX, ^FVX, ^TYX

Total initial symbols: 19  
Target scale: 30–50 symbols after performance validation.



# Generate a valid Fernet key on your host
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

### Project runbook — start to finish (numbered steps with commands)

Prerequisites
- Docker and Docker Compose installed.
- Run from repository root.
- Confirm these paths exist: `scripts/`, `schemas/market_tick.avsc`, `flink/conf/`, `flink/lib/`.
- Recommended: rpk (Redpanda CLI) or use rpk inside the Redpanda container, and mc (MinIO client) or MinIO web UI.

1) Stop and clean any existing stack
```bash
docker compose down
# optional full clean (removes volumes and orphan containers)
docker compose down -v --remove-orphans
```

2) Ensure Flink S3A JARs are present (host)
```bash
mkdir -p flink/lib
curl -L -o flink/lib/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -L -o flink/lib/aws-java-sdk-bundle-1.12.262.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
ls -la flink/lib
```

3) Build and bring up the full stack
```bash
docker compose up -d --build
```

4) Confirm services are running
```bash
docker compose ps
# tail logs if needed
docker compose logs -f --tail 200 redpanda minio postgres airflow flink-jobmanager flink-taskmanager
```

5) Recreate Flink services (ensure jars/config mounted)
```bash
docker compose up -d --build flink-jobmanager flink-taskmanager
docker compose ps --filter "name=flink"
```

6) Initialize and start Airflow (if not already)
```bash
# Run init (creates DB + admin user) if defined
docker compose up -d airflow-init
# Start Airflow webserver + scheduler
docker compose up -d airflow
# (optional) build if Dockerfile changed
docker compose build airflow airflow-init
# check airflow
docker compose logs -f airflow
```

7) Verify Kafka topics (init-topics)
```bash
# inside Redpanda container; adjust container name if needed
docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic list --brokers=redpanda:9092
# expect: crypto_ticks, equities_ticks, bonds_data
```

8) Run scrapers manually for quick tests (inside Airflow container)
```bash
# open shell in Airflow container
docker compose exec airflow bash

# from inside container run each script
python -u /opt/airflow/scripts/bonds_eod_writer.py
python -u /opt/airflow/scripts/crypto_producer.py
python -u /opt/airflow/scripts/equities_eod_writer.py
python -u /opt/airflow/scripts/equities_intraday_producer.py
python -u /opt/airflow/scripts/all_tickers_scraper.py
```

9) Trigger DAGs via Airflow CLI (one at a time)
```bash
docker compose exec airflow bash -lc "airflow dags trigger bonds_eod_batch_dag"
docker compose exec airflow bash -lc "airflow dags trigger crypto_stream_dag"
docker compose exec airflow bash -lc "airflow dags trigger equities_eod_batch_dag"
docker compose exec airflow bash -lc "airflow dags trigger equities_intraday_stream_dag"
docker compose exec airflow bash -lc "airflow dags trigger full_pipeline_test_dag"
# test a DAG run
docker compose exec airflow bash -lc "airflow dags test bonds_eod_batch_dag 2025-09-20"
```

10) Validate Kafka topics have messages
```bash
# crypto ticks
docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic consume crypto_ticks --brokers=redpanda:9092 --num 5
# equities intraday ticks
docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic consume equities_ticks --brokers=redpanda:9092 --num 5
# bonds_data (only if you chose to produce bonds into Kafka)
docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic consume bonds_data --brokers=redpanda:9092 --num 5
```

11) List MinIO EOD uploads (batch Parquet)
```bash
# use mc inside MinIO container
docker exec -it $(docker ps -q --filter "name=minio") sh -lc \
"mc alias set local http://minio:9000 minio minio123 >/dev/null 2>&1 || true; mc ls local/quant/eod --recursive"
# Or open MinIO UI: http://localhost:9001 (user=minio, pass=minio123)
```

12) Submit the PyFlink streaming job (recommended submitter container)
```bash
# replace 'quantnet' with your docker compose network name if different
docker run --rm --network quantnet -v "$(pwd)/scripts":/app/scripts -v "$(pwd)/schemas":/app/schemas python:3.10-slim bash -lc \
"pip install --no-cache-dir pyflink==1.17.1 fastavro==1.9.4 && python /app/scripts/flink_jobs/stream_market_to_avro.py"
# alternative if your Flink image has Python:
docker compose exec -T flink-jobmanager bash -lc "python /opt/flink/app/scripts/flink_jobs/stream_market_to_avro.py"
```

13) Confirm Flink job is running
```bash
# Flink CLI inside jobmanager
docker compose exec flink-jobmanager /opt/flink/bin/flink list
# Or open Flink UI: http://localhost:8082 — check job "Market streaming TA → Avro"
```

14) Verify Flink is consuming from Kafka
```bash
# check topic offsets / lag (Redpanda)
docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic offsets crypto_ticks --brokers=redpanda:9092
# check Redpanda Console UI for consumer groups and lag: http://localhost:8081
```

15) List Avro outputs in MinIO (streaming sink)
```bash
docker exec -it $(docker ps -q --filter "name=minio") sh -lc \
"mc alias set local http://minio:9000 minio minio123 >/dev/null 2>&1 || true; mc ls local/quant/streaming --recursive"
# Open MinIO UI: http://localhost:9001 and browse bucket 'quant' → streaming
```

16) Copy a sample Avro file from MinIO and inspect locally
```bash
# adjust the path based on mc ls output
docker exec -it $(docker ps -q --filter "name=minio") sh -lc \
"mc cp local/quant/streaming/market_avro/date=YYYY-MM-DD/symbol=BTC-USD/part-00000.avro /tmp/part-00000.avro"
docker cp $(docker ps -q --filter "name=minio"):/tmp/part-00000.avro ./part-00000.avro

# inspect with fastavro locally
pip install fastavro
python - <<'PY'
from fastavro import reader
with open("part-00000.avro","rb") as f:
    r = reader(f)
    for i, rec in enumerate(r):
        print(rec)
        if i >= 2:
            break
PY
```

17) (Optional) Inspect Parquet outputs (if Flink writes Parquet)
```bash
# list parquet files in MinIO
docker exec -it $(docker ps -q --filter "name=minio") sh -lc "mc ls local/quant --recursive | grep '.parquet' || true"

# copy a parquet file locally and inspect
docker exec -it $(docker ps -q --filter "name=minio") sh -lc \
"mc cp local/quant/streaming/.../part-00000.parquet /tmp/part-00000.parquet"
docker cp $(docker ps -q --filter "name=minio"):/tmp/part-00000.parquet ./part-00000.parquet

pip install pyarrow pandas
python - <<'PY'
import pyarrow.parquet as pq
df = pq.read_table("part-00000.parquet").to_pandas()
print(df.head())
PY
```

18) Quick smoke-produce test (one message) and re-check MinIO
```bash
# produce one test message into crypto_ticks (in Redpanda container)
docker exec -i $(docker ps -q --filter "name=redpanda") bash -lc "python - <<'PY'
import json, time
from confluent_kafka import Producer
p=Producer({'bootstrap.servers':'redpanda:9092'})
msg={'symbol':'SMOKE-USD','timestamp':time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime()),'open':1.0,'high':1.0,'low':1.0,'close':1.0,'volume':0.0,'ingested_at':time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime())}
p.produce('crypto_ticks', json.dumps(msg).encode('utf-8'))
p.flush()
print('produced')
PY"
# wait 10-30s then inspect MinIO streaming path again (step 15)
```

19) Troubleshooting commands

# tail Flink logs
docker compose logs --tail 200 flink-jobmanager
docker compose logs --tail 200 flink-taskmanager

# show Airflow logs
docker compose logs --tail 200 airflow

# list files in flink lib and show Flink conf
ls -la ./flink/lib
cat ./flink/conf/flink-conf.yaml
cat ./flink/conf/core-site.xml

# if docker exec fails with namespace errors, recreate containers
docker compose rm -fs flink-jobmanager flink-taskmanager
docker compose up -d --build flink-jobmanager flink-taskmanager
```

20) Tear down when finished

docker compose down
# remove volumes and orphans for a completely clean start
docker compose down -v --remove-orphans


Follow these numbered steps in order. If any command fails, copy the error output and relevant container logs and I will provide the next corrective command.




# MongoDB:
Username: hobrian2004_db_user
Password: W6ONKjcUrUzkiNJG

# install ngrok
brew install ngrok/ngrok/ngrok

# my ngrok recovery code:
4KDZRDF8YU
VHRER2NSNJ
YUTXK43VW7
7XRJY772NB
YRBW2EM5Z7
V75NV8BSQK
M83B9ME3R8
8TSFCWSQN7
UHWT5JKB5A
UZ2FRCFH6X

# Authen ngrok
ngrok config add-authtoken 32sBFDUu6SLfsYAJeXorfYSxG3P_5ZJUACTmBERtzrPWaufCx


# Kill Ngrok
docker compose stop ngrok
pkill -f ngrok

# Test exposing Kafka port 9092
ngrok tcp 9092
