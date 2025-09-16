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
docker compose up -d airflow

# Check airflow:
docker compose logs -f airflow

# MongoDB:
Username: hobrian2004_db_user
Password: W6ONKjcUrUzkiNJG