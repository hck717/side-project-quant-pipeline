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

# Stop current stack
docker compose down

# Bring it up all services in docker: 
docker-compose up -d

**remarks: ensure Redpanada image to be: image: redpandadata/redpanda:v23.3.5 or latest 

# Rebuild  docker: 
docker compose down -v --remove-orphans

# Check services
docker compose ps

# Bring up airflow:
docker compose up airflow-init
docker compose up -d airflow
docker compose build airflow
docker compose build airflow-init


# go into airflow container
docker exec -it airflow /bin/bash
docker compose run --entrypoint bash airflow


# Check airflow:
docker compose logs -f airflow

# Verfiy airflow DAGs
docker compose exec redpanda rpk topic consume crypto.ticks -n 5

# Restart airflow
docker compose restart airflow

# 1. List your running containers to confirm the Airflow service name
docker ps

# 2. Exec into the Airflow container (replace 'airflow' if your container name differs)
docker compose run --entrypoint bash airflow

# 3. Inside the container, trigger each DAG manually, one at a time:
airflow dags trigger bonds_eod_batch_dag
airflow dags trigger crypto_stream_dag
airflow dags trigger equities_eod_batch_dag
airflow dags trigger equities_intraday_stream_dag
airflow dags trigger full_pipeline_test_dag
airflow dags test bonds_eod_batch_dag 2025-09-20


# 4. (Optional) Watch task logs for a DAG run
airflow tasks logs bonds_eod_batch_dag batch_bonds_eod <run_id>

# 1. Get into the Airflow container shell
docker exec -it airflow bash
docker compose run --entrypoint bash airflow

# 2. Inside the container, run each scraper one by one:

# Bonds EOD → MinIO
python -u /opt/airflow/scripts/bonds_eod_writer.py

# Crypto intraday → Kafka
python -u /opt/airflow/scripts/crypto_producer.py

# Equities EOD → MinIO
python /opt/airflow/scripts/equities_eod_writer.py

# Equities intraday → Kafka
python /opt/airflow/scripts/equities_intraday_producer.py

# All tickers combined → MinIO + Kafka
python /opt/airflow/scripts/all_tickers_scraper.py


# *** Reference fixing yfinance rate limit --> need VPN / yfinance version > 2.58
https://blog.csdn.net/weixin_43252521/article/details/148328355


# *** check for yfinance rate limit
curl -v https://query1.finance.yahoo.com/v8/finance/chart/AAPL

# Check that logging is working in your scripts
docker compose run --entrypoint bash airflow
python -u /opt/airflow/scripts/crypto_producer.py

# Check that the Kafka topics exist
docker exec -it side-project-quant-pipeline-redpanda-1 rpk topic list --brokers=redpanda:9092

# check if the Kafka topics received data ingestion
docker exec -it side-project-quant-pipeline-redpanda-1 \
  rpk topic consume crypto_ticks --brokers=redpanda:9092 --num 5

# Build PyFlink
docker compose build flink-jobmanager flink-taskmanager


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
