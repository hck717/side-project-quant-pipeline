# Quant Pipeline Side Project

A scalable data pipeline for processing real-time and batch financial market data, including crypto, equities, and bonds, using Dockerized services (Airflow, Redpanda, MinIO, MongoDB) with Python for scraping and processing.

## Architecture Overview
The pipeline orchestrates:
- **Airflow**: Runs four Python scripts to scrape data from `yfinance` for equities (EOD, intraday), crypto, and bonds.
- **Kafka (Redpanda)**: Streams real-time data to topics (`crypto_ticks`, `equities_ticks`, `bonds_data`).
- **Python Processing Services**:
  - **Real-time**: `aiokafka` consumes from Kafka, computes TA/quant metrics (pandas, NumPy, TA-Lib), writes to MongoDB.
  - **Batch**: MinIO SDK reads historical files, computes metrics, writes to MongoDB.
- **MinIO**: Stores raw historical/batch data (Parquet/CSV, partitioned by date/asset/symbol).
- **MongoDB Atlas M0**: Persists processed/enriched data with indexes for fast queries.
- **Monitoring**: Airflow UI (port 8080), Redpanda Console (8081), MinIO Console (9001). Latency tracked via Python timers.

## Data Scope (Sprint 0)
- **Crypto (real-time, 1m/5m)**: BTC-USD, ETH-USD, SOL-USD, ADA-USD, XRP-USD
- **Equities (intraday, 5-min)**: AAPL, MSFT, AMZN, TSLA, NVDA
- **Equities (EOD)**: AAPL, MSFT, AMZN, TSLA, NVDA, JPM, XOM, META, GOOGL, NFLX
- **Bonds (EOD)**: ^TNX, ^IRX, ^FVX, ^TYX
- **Total initial symbols**: 19
- **Target scale**: 30–50 symbols after performance validation

## Data Sources
- **Library**: `yfinance==0.2.61`
- **Equities**: Daily EOD + 5-min intraday
- **Crypto**: 1-min/5-min intraday
- **Bonds**: Daily U.S. Treasury proxies
- **Scraping**: Four local Python scripts (`equities_eod_scraper.py`, `equities_intraday_scraper.py`, `crypto_scraper.py`, `bonds_scraper.py`) produce to local Kafka.

## Data Transformations
### Raw Market Data (yfinance)
- Datetime/Timestamp
- Open, High, Low, Close (adjusted/unadjusted), Volume
- Optional: Ticker, Currency, Exchange (for bonds/crypto)

### Computed TA/Quant Metrics
- **Trend/Momentum**: SMA (20d, 50d, 200d), EMA, MACD, ROC, RSI, Momentum
- **Volatility**: Bollinger Bands, ATR, Historical Volatility
- **Volume-based**: OBV, VWAP, Accumulation/Distribution
- **Quant/Statistical**: Log Return, Cumulative Return, Sharpe/Sortino Ratios, Drawdown, Beta, Correlations
- **Cross-Asset**: Spreads, Yield Curve Slope, Crypto/Equity Correlations

## Data Destinations
- **MinIO**: Historical data (Parquet, partitioned by date/asset/symbol)
  - Equities EOD, Bonds EOD, Batch exports
- **Kafka (Redpanda)**: Streaming data
  - `crypto_ticks`: Crypto intraday (1m/5m)
  - `equities_ticks`: Equities intraday (5m)
  - `bonds_data`: Bonds EOD (optional)
- **MongoDB**: Processed/enriched data with indexes

## Prerequisites
### Local Installs (macOS)
- **Docker Desktop**: Runs containers
- **VS Code**: IDE with extensions:
  - Docker
  - Dev Containers
  - Python
  - YAML
- **Git**: Repository management
- **Optional Tools**:
  - `rpk`: Redpanda CLI (or use inside Redpanda container)
  - `mc`: MinIO CLI (or use MinIO web UI: http://localhost:9001, user: `minio`, pass: `minio123`)

### File Paths
Ensure these exist in the repository root:
- `scripts/`
- `schemas/market_tick.avsc`
- `flink/conf/`
- `flink/lib/`

### Generate Fernet Key
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Setup Instructions
### Initial Setup
1. Install dependencies:
   ```bash
   brew install librdkafka
   pip install -r scripts/requirements-processing.txt
   ```
2. Create GitHub repo with `.gitignore` for Python, Airflow, Docker, `.env`.
3. Set up MongoDB Atlas M0 cluster, create database/collections.
4. Create `docker-compose.yml` with:
   - Airflow (LocalExecutor)
   - Redpanda (Kafka)
   - MinIO
   - MongoDB
5. Push initial stack to GitHub.
6. Document architecture in `ARCHITECTURE.md`.

### Validation
- All containers start without errors.
- UIs accessible:
  - Airflow: http://localhost:8080
  - Redpanda Console: http://localhost:8081
  - MinIO: http://localhost:9001
- MongoDB accessible via Atlas UI or MongoDB Compass.

## Sprint 0: Environment Setup (Day 1–2)
**Goal**: Set up local Docker stack, repository, and documentation.

### Commands
1. Stop and clean existing stack:
   ```bash
   docker compose down
   # Optional full clean (removes volumes/orphans):
   docker compose down -v --remove-orphans
   ```
2. Build and start stack:
   ```bash
   docker compose up -d --build
   ```
3. Confirm services are running:
   ```bash
   docker compose ps
   # Tail logs if needed:
   docker compose logs -f --tail 200 redpanda minio postgres airflow
   ```

### Deliverables
- Local Docker stack (Airflow, Redpanda, MinIO)
- MongoDB Atlas M0 cluster
- GitHub repo with README, `.gitignore`
- Architecture diagram in `ARCHITECTURE.md`

### Validation
- Containers start without errors.
- UIs and MongoDB accessible.

## Sprint 1: Orchestration & Ingestion (Day 3–5)
**Goal**: Build Airflow DAGs to trigger four Python scripts for `yfinance` scraping and Kafka production.

### Commands
1. Initialize and start Airflow:
   ```bash
   # Run init (creates DB + admin user):
   docker compose up -d airflow-init
   # Start Airflow webserver + scheduler:
   docker compose up -d airflow
   # Optional: rebuild if Dockerfile changed:
   docker compose build airflow airflow-init
   # Check logs:
   docker compose logs -f airflow
   ```
2. Verify Kafka topics:
   ```bash
   docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic list --brokers=redpanda:9092
   # Expected: crypto_ticks, equities_ticks, bonds_data
   ```
3. Run scrapers manually (inside Airflow container):
   ```bash
   docker compose exec airflow bash
   # Inside container:
   python -u /opt/airflow/scripts/bonds_eod_writer.py
   python -u /opt/airflow/scripts/crypto_producer.py
   python -u /opt/airflow/scripts/equities_eod_writer.py
   python -u /opt/airflow/scripts/equities_intraday_producer.py
   python -u /opt/airflow/scripts/all_tickers_scraper.py
   ```
4. Trigger DAGs via Airflow CLI:
   ```bash
   docker compose exec airflow bash -lc "airflow dags trigger bonds_eod_batch_dag"
   docker compose exec airflow bash -lc "airflow dags trigger crypto_stream_dag"
   docker compose exec airflow bash -lc "airflow dags trigger equities_eod_batch_dag"
   docker compose exec airflow bash -lc "airflow dags trigger equities_intraday_stream_dag"
   docker compose exec airflow bash -lc "airflow dags trigger full_pipeline_test_dag"
   # Test a DAG run:
   docker compose exec airflow bash -lc "airflow dags test bonds_eod_batch_dag 2025-09-20"
   ```
5. Validate Kafka topics have messages:
   ```bash
   docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic consume crypto_ticks --brokers=redpanda:9092 --num 5
   docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic consume equities_ticks --brokers=redpanda:9092 --num 5
   docker exec -it $(docker ps -q --filter "name=redpanda") rpk topic consume bonds_data --brokers=redpanda:9092 --num 5
   ```
6. List MinIO EOD uploads:
   ```bash
   docker exec -it $(docker ps -q --filter "name=minio") sh -lc \
   "mc alias set local http://minio:9000 minio minio123 >/dev/null 2>&1 || true; mc ls local/quant/eod --recursive"
   # Or use MinIO UI: http://localhost:9001 (user: minio, pass: minio123)
   ```

### Deliverables
- Four Python scripts (`equities_eod_scraper.py`, `equities_intraday_scraper.py`, `crypto_scraper.py`, `bonds_scraper.py`)
- Four Airflow DAGs triggering scripts
- Kafka topics created and populated

### Validation
- DAGs run manually; messages appear in Kafka (Redpanda Console).
- Scripts log `[SCRAPE DEBUG]` with timings and success/failure.
- Topics populated with JSON matching schema.

## Sprint 2: Streaming & Batch Processing (Day 6–8)
**Goal**: Replace Flink with Python services for real-time and batch processing, writing to MongoDB.

### Commands
1. Start MongoDB, Redpanda, MinIO:
   ```bash
   docker compose up -d mongodb redpanda minio
   ```
2. Build processor images:
   ```bash
   docker compose build stream-processor batch-processor
   ```
3. Run stream processor:
   ```bash
   docker compose up stream-processor
   ```
4. Run batch processor:
   ```bash
   docker compose run --rm batch-processor
   # Process specific date and asset type:
   docker compose run --rm batch-processor python scripts/processing/batch_processor.py --date 2025-09-23 --asset-type equities
   ```
5. Verify and analyze MongoDB indexes:
   ```bash
   docker compose run --rm mongo-tools python scripts/processing/mongo_index_manager.py --verify --analyze
   ```
6. Run query benchmarks:
   ```bash
   docker compose run --rm mongo-tools python scripts/processing/query_benchmark.py
   ```
7. Add TTL index (optional):
   ```bash
   docker compose run --rm mongo-tools python scripts/processing/mongo_index_manager.py --add-ttl --ttl-days 30
   ```
8. Clean up MongoDB (if needed):
   ```bash
   docker compose run --rm mongo-tools python scripts/processing/mongo_consolidate.py
   ```

### Deliverables
- Real-time processor: `aiokafka` → TA/quant metrics → MongoDB
- Batch processor: MinIO SDK → TA/quant metrics → MongoDB
- Shared TA/quant functions
- Logging and error handling

### Validation
- MongoDB collections populated with processed data.
- Indexes: `{symbol: 1, timestamp: 1}`.
- Logs show processing latency and write success.

## Sprint 3: Storage & Serving (Day 9–11)
**Goal**: Optimize MongoDB storage and query performance.

### Commands
- (Covered in Sprint 2: index verification, query benchmarks, TTL index)

### Deliverables
- Indexed MongoDB collections
- Sample queries returning in <200 ms

### Validation
- Queries return expected rows quickly.
- MongoDB Compass shows correct indexes.

## Notes
- **Flink Failure**: Flink was replaced due to complex JAR/Dockerfile setup.
- **Colab/ngrok Failure**: Original Colab + GitHub Actions + ngrok setup failed due to ngrok timeouts, Colab runtime limits, and GitHub Actions latency. Replaced with local Python scripts and Airflow DAGs for reliability.

## Troubleshooting
- Check container logs: `docker compose logs -f <service>`
- Verify Kafka topics: Use Redpanda Console or `rpk` commands
- Monitor MinIO: Use UI or `mc` commands
- Ensure MongoDB Atlas connection is active
- Review Airflow logs for DAG failures

---

This README consolidates all provided information, organizes commands by sprint, and ensures clarity for setup and execution. Let me know if you need further refinements or additional sections!

