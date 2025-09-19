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
docker-compose build --no-cache devcontainer

# Check services
docker compose ps

# Bring up airflow:
docker compose up airflow-init
docker compose up -d airflow
docker compose build airflow
docker compose build airflow-init


# go into airflow container
docker exec -it airflow /bin/bash

# Check airflow:
docker compose logs -f airflow

# Verfiy airflow DAGs
docker compose exec redpanda rpk topic consume crypto.ticks -n 5

# Restart airflow
docker compose restart airflow

# create Kafka Topics once
docker compose exec redpanda rpk topic create crypto.ticks \
  --partitions 2 --replicas 1 --config retention.ms=3600000

docker compose exec redpanda rpk topic create equities.intraday \
  --partitions 2 --replicas 1 --config retention.ms=3600000

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
