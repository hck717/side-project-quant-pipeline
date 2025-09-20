import yfinance as yf
import pandas as pd
from minio import Minio
from confluent_kafka import Producer
from datetime import datetime, timezone
import json
from tenacity import retry, stop_after_attempt, wait_fixed

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "quant"

KAFKA_BROKER = "redpanda:9092"

BOND_SYMBOLS = ["^TNX", "^IRX", "^FVX", "^TYX"]
CRYPTO_PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD"]
EQUITY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA", "JPM", "XOM", "META", "GOOGL", "NFLX"]
EQUITY_INTRADAY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]

# Updated topic names to match underscore convention
TOPIC_CRYPTO = "crypto_ticks"
TOPIC_EQUITIES_INTRADAY = "equities_ticks"

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_data(symbol, period="1d", interval="1d"):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=period, interval=interval)
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df.iloc[-1:]  # Return only the latest row

def run_all_tickers_scraper():
    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                         secret_key=MINIO_SECRET_KEY, secure=False)

    # Ensure bucket exists
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)

    kafka_producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    success = False

    # Bonds EOD → MinIO
    for sym in BOND_SYMBOLS:
        try:
            df = fetch_data(sym, period="1d", interval="1d")
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date
            print(f"[SCRAPE DEBUG] Bonds {sym} latest data: {df.to_dict(orient='records')}")
            file_name = f"{sym}.parquet"
            df.to_parquet(file_name, index=False)
            s3_path = f"eod/bonds/date={df['date'].iloc[0]}/{file_name}"
            minio_client.fput_object(BUCKET, s3_path, file_name)
            success = True
        except Exception as e:
            print(f"[SCRAPE ERROR] Bonds {sym} failed: {str(e)}")

    # Equities EOD → MinIO
    for sym in EQUITY_SYMBOLS:
        try:
            df = fetch_data(sym, period="1d", interval="1d")
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date
            print(f"[SCRAPE DEBUG] Equities EOD {sym} latest data: {df.to_dict(orient='records')}")
            file_name = f"{sym}.parquet"
            df.to_parquet(file_name, index=False)
            s3_path = f"eod/equities/date={df['date'].iloc[0]}/{file_name}"
            minio_client.fput_object(BUCKET, s3_path, file_name)
            success = True
        except Exception as e:
            print(f"[SCRAPE ERROR] Equities EOD {sym} failed: {str(e)}")

    # Crypto intraday → Kafka
    for sym in CRYPTO_PAIRS:
        try:
            df = fetch_data(sym, period="1d", interval="1m")
            latest_row = df.iloc[-1]
            msg = {
                "symbol": sym,
                "timestamp": latest_row.name.isoformat(),
                "open": float(latest_row["Open"]),
                "high": float(latest_row["High"]),
                "low": float(latest_row["Low"]),
                "close": float(latest_row["Close"]),
                "volume": float(latest_row["Volume"]),
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }
            print(f"[SCRAPE DEBUG] Crypto {sym} latest data: {msg}")
            kafka_producer.produce(TOPIC_CRYPTO, json.dumps(msg).encode('utf-8'))
            success = True
        except Exception as e:
            print(f"[SCRAPE ERROR] Crypto {sym} failed: {str(e)}")

    # Equities intraday → Kafka
    for sym in EQUITY_INTRADAY_SYMBOLS:
        try:
            df = fetch_data(sym, period="1d", interval="5m")
            latest_row = df.iloc[-1]
            msg = {
                "symbol": sym,
                "timestamp": latest_row.name.isoformat(),
                "open": float(latest_row["Open"]),
                "high": float(latest_row["High"]),
                "low": float(latest_row["Low"]),
                "close": float(latest_row["Close"]),
                "volume": float(latest_row["Volume"]),
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }
            print(f"[SCRAPE DEBUG] Equities Intraday {sym} latest data: {msg}")
            kafka_producer.produce(TOPIC_EQUITIES_INTRADAY, json.dumps(msg).encode('utf-8'))
            success = True
        except Exception as e:
            print(f"[SCRAPE ERROR] Equities Intraday {sym} failed: {str(e)}")

    kafka_producer.flush()
    if not success:
        raise RuntimeError("No data fetched successfully")
