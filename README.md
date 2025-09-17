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


# Bring it up all services in docker: 
docker-compose up -d

**remarks: ensure Redpanada image to be: image: redpandadata/redpanda:v23.3.5 or latest 

# Rebuild  docker: 
docker-compose build --no-cache devcontainer

# Bring up airflow:
docker compose build airflow
docker compose up -d airflow

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


# Kafka (Redpanda)
ngrok tcp 9092

# MinIO API
ngrok http 9000

# Setup Cloudfare
# 1. Install cloudflared (Mac example)
brew install cloudflared

# 2. Authenticate with Cloudflare
cloudflared tunnel login

# 3. Create a tunnel
cloudflared tunnel create my-pipeline-tunnel

# 4. Map DNS hostnames to the tunnel
cloudflared tunnel route dns my-pipeline-tunnel kafka.mydomain.com
cloudflared tunnel route dns my-pipeline-tunnel minio.mydomain.com

# 5. Edit the config file
nano ~/.cloudflared/config.yml