import yfinance as yf
import pandas as pd
import logging
from datetime import datetime
from minio import Minio
import os

EQUITY_SYMBOLS = ["AAPL", "MSFT", "GOOG"]
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "quant"

def run_equities_eod_writer():
    logging.info("[equities_eod_writer] Fetching equities EOD data...")
    df = yf.download(EQUITY_SYMBOLS, period="1mo", interval="1d", group_by='ticker', threads=True)
    if df.empty:
        logging.warning("[equities_eod_writer] No data fetched.")
        return

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    for sym in EQUITY_SYMBOLS:
        try:
            sym_df = df[sym].reset_index().dropna(subset=["Date"])
            if sym_df.empty:
                continue
            latest_date = sym_df["Date"].max().date()
            latest_rows = sym_df[sym_df["Date"].dt.date == latest_date]
            file_name = f"{sym}.parquet"
            tmp_path = f"/tmp/{file_name}"
            latest_rows.to_parquet(tmp_path, index=False)
            s3_path = f"eod/equities/date={latest_date}/{file_name}"
            client.fput_object(BUCKET, s3_path, tmp_path)
            logging.info(f"[equities_eod_writer] Wrote {len(latest_rows)} rows to s3://{BUCKET}/{s3_path}")
        except KeyError:
            logging.warning(f"[equities_eod_writer] No data for {sym}")

if __name__ == "__main__":
    run_equities_eod_writer()
