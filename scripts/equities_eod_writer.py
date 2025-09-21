#!/usr/bin/env python3
import os
import yfinance as yf
import pandas as pd
import logging
from minio import Minio
from minio.error import S3Error
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_fixed

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "quant"
EQUITY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA", "JPM", "XOM", "META", "GOOGL", "NFLX"]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_equity_data(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="1d", interval="1d")
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df.iloc[-1:]

def ensure_bucket(client: Minio, bucket_name: str):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info(f"Created bucket: {bucket_name}")
    except S3Error as e:
        # Ignore the case where bucket already exists / owned by you
        if getattr(e, "code", None) == "BucketAlreadyOwnedByYou":
            logging.info(f"Bucket {bucket_name} already owned, continuing")
        else:
            logging.error(f"MinIO error when ensuring bucket: {e}")
            raise

def run_equities_eod_writer():
    start_time = datetime.now(timezone.utc)
    logging.info(f"Starting equities EOD scrape at {start_time.isoformat()}")

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)

    ensure_bucket(client, BUCKET)

    success_any = False
    tmp_files = []

    for sym in EQUITY_SYMBOLS:
        try:
            df = fetch_equity_data(sym)
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()

            logging.info(f"[API OK] {sym} latest data: {df.to_dict(orient='records')}")

            file_name = f"{sym}.parquet"
            # Use a temp local path to avoid partial uploads
            tmp_path = f"/tmp/{sym}.{int(start_time.timestamp())}.parquet"
            tmp_files.append(tmp_path)
            df.to_parquet(tmp_path, index=False)

            # MinIO path partitioning
            s3_path = f"eod/equities/date={df['date'].iloc[0]}/{file_name}"

            try:
                client.fput_object(BUCKET, s3_path, tmp_path)
                logging.info(f"[UPLOAD OK] {sym} saved to s3://{BUCKET}/{s3_path}")
                success_any = True
            except S3Error as e:
                # If bucket exists race, ignore; otherwise rethrow after logging
                if getattr(e, "code", None) == "BucketAlreadyOwnedByYou":
                    logging.info(f"BucketAlreadyOwnedByYou during upload for {sym}, continuing")
                    success_any = True
                else:
                    logging.error(f"[UPLOAD ERROR] {sym} failed to upload: {e}")
                    raise

        except Exception as e:
            logging.error(f"[SCRAPE ERROR] {sym} failed: {str(e)}")

    # cleanup tmp files
    for p in tmp_files:
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    end_time = datetime.now(timezone.utc)
    elapsed = (end_time - start_time).total_seconds()
    if success_any:
        logging.info(f"Equities EOD scrape completed in {elapsed:.2f} seconds")
    else:
        logging.error("Equities EOD scrape failed for all symbols")
        raise RuntimeError("No equities EOD data fetched successfully")

if __name__ == "__main__":
    run_equities_eod_writer()

