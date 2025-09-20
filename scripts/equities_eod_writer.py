import yfinance as yf
import pandas as pd
import logging
from minio import Minio
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

def run_equities_eod_writer():
    start_time = datetime.now(timezone.utc)
    logging.info(f"Starting equities EOD scrape at {start_time.isoformat()}")

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    success = False
    for sym in EQUITY_SYMBOLS:
        try:
            df = fetch_equity_data(sym)
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()
            logging.info(f"[API OK] {sym} latest data: {df.to_dict(orient='records')}")
            file_name = f"{
