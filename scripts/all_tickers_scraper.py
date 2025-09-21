#!/usr/bin/env python3
import os
import json
import yfinance as yf
import pandas as pd
import logging
from minio import Minio
from minio.error import S3Error
from confluent_kafka import Producer
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_fixed

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "quant"

KAFKA_BROKER = "redpanda:9092"

BOND_SYMBOLS = ["^TNX", "^IRX", "^FVX", "^TYX"]
CRYPTO_PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD"]
EQUITY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA", "JPM", "XOM", "META", "GOOGL", "NFLX"]
EQUITY_INTRADAY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]

TOPIC_CRYPTO = "crypto_ticks"
TOPIC_EQUITIES_INTRADAY = "equities_ticks"

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_data(symbol, period="1d", interval="1d"):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=period, interval=interval)
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df

def ensure_bucket(client: Minio, bucket_name: str):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info(f"Created bucket: {bucket_name}")
    except S3Error as e:
        if getattr(e, "code", None) == "BucketAlreadyOwnedByYou":
            logging.info(f"Bucket {bucket_name} already owned, continuing")
        else:
            logging.error(f"MinIO error when ensuring bucket: {e}")
            raise

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Kafka delivery failed for {msg.key()}: {err}")
    else:
        logging.info(f"Produced to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")

def run_all_tickers_scraper():
    start_time = datetime.now(timezone.utc)
    logging.info(f"Starting full scrape at {start_time.isoformat()}")

    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                         secret_key=MINIO_SECRET_KEY, secure=False)

    ensure_bucket(minio_client, BUCKET)

    kafka_producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    success = False
    tmp_files = []

    # Bonds EOD → MinIO
    for sym in BOND_SYMBOLS:
        try:
            df = fetch_data(sym, period="1d", interval="1d")
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()
            logging.info(f"[SCRAPE DEBUG] Bonds {sym} latest data: {df.to_dict(orient='records')}")
            file_name = f"{sym}.parquet"
            tmp_path = f"/tmp/{sym}.{int(start_time.timestamp())}.parquet"
            tmp_files.append(tmp_path)
            df.to_parquet(tmp_path, index=False)
            s3_path = f"eod/bonds/date={df['date'].iloc[0]}/{file_name}"
            try:
                minio_client.fput_object(BUCKET, s3_path, tmp_path)
                logging.info(f"[UPLOAD OK] Bonds {sym} -> s3://{BUCKET}/{s3_path}")
                success = True
            except S3Error as e:
                if getattr(e, "code", None) == "BucketAlreadyOwnedByYou":
                    logging.info("BucketAlreadyOwnedByYou during bonds upload, continuing")
                    success = True
                else:
                    logging.error(f"[UPLOAD ERROR] Bonds {sym} failed to upload: {e}")
                    raise
        except Exception as e:
            logging.error(f"[SCRAPE ERROR] Bonds {sym} failed: {str(e)}")

    # Equities EOD → MinIO
    for sym in EQUITY_SYMBOLS:
        try:
            df = fetch_data(sym, period="1d", interval="1d")
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()
            logging.info(f"[SCRAPE DEBUG] Equities EOD {sym} latest data: {df.to_dict(orient='records')}")
            file_name = f"{sym}.parquet"
            tmp_path = f"/tmp/{sym}.{int(start_time.timestamp())}.parquet"
            tmp_files.append(tmp_path)
            df.to_parquet(tmp_path, index=False)
            s3_path = f"eod/equities/date={df['date'].iloc[0]}/{file_name}"
            try:
                minio_client.fput_object(BUCKET, s3_path, tmp_path)
                logging.info(f"[UPLOAD OK] Equities EOD {sym} -> s3://{BUCKET}/{s3_path}")
                success = True
            except S3Error as e:
                if getattr(e, "code", None) == "BucketAlreadyOwnedByYou":
                    logging.info("BucketAlreadyOwnedByYou during equities upload, continuing")
                    success = True
                else:
                    logging.error(f"[UPLOAD ERROR] Equities EOD {sym} failed to upload: {e}")
                    raise
        except Exception as e:
            logging.error(f"[SCRAPE ERROR] Equities EOD {sym} failed: {str(e)}")

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
            logging.info(f"[SCRAPE DEBUG] Crypto {sym} latest data: {msg}")
            kafka_producer.produce(TOPIC_CRYPTO, json.dumps(msg).encode('utf-8'), callback=delivery_report)
            success = True
        except Exception as e:
            logging.error(f"[SCRAPE ERROR] Crypto {sym} failed: {str(e)}")

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
            logging.info(f"[SCRAPE DEBUG] Equities Intraday {sym} latest data: {msg}")
            kafka_producer.produce(TOPIC_EQUITIES_INTRADAY, json.dumps(msg).encode('utf-8'), callback=delivery_report)
            success = True
        except Exception as e:
            logging.error(f"[SCRAPE ERROR] Equities Intraday {sym} failed: {str(e)}")

    kafka_producer.flush()

    # cleanup tmp files
    for p in tmp_files:
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    logging.info(f"Finished full scrape at {end_time.isoformat()} (duration: {duration})")

    if not success:
        raise RuntimeError("No data fetched successfully")

if __name__ == "__main__":
    run_all_tickers_scraper()
