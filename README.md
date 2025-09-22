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


3) Build and bring up the full stack
```bash
docker compose up -d --build
```

4) Confirm services are running
```bash
docker compose ps
# tail logs if needed
docker compose logs -f --tail 200 redpanda minio postgres airflow 
```

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

# *** librdkafka for confluent.kafka
brew install librdkafka

# Install the new requirements for sprint2: 
pip install -r scripts/requirements-processing.txt

# Start MongoDB in docker-compose:
docker-compose up -d mongodb

# run stream process
cd scripts/processing
python stream_processor.pydocker-compose up -d mongodb

# run batch processing
cd scripts/processing
python batch_processor.py --asset-type all


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
