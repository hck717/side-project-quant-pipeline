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
BOND_SYMBOLS = ["^TNX", "^IRX", "^FVX", "^TYX"]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_bond_data(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="1d", interval="1d")
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df.iloc[-1:]

def run_bonds_eod_writer():
    start_time = datetime.now(timezone.utc)
    logging.info(f"Starting bonds EOD scrape at {start_time.isoformat()}")

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    success = False
    for sym in BOND_SYMBOLS:
        try:
            df = fetch_bond_data(sym)
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()
            logging.info(f"[API OK] {sym} latest data: {df.to_dict(orient='records')}")
            file_name = f"{sym.replace('^','')}.parquet"
            s3_path = f"eod/bonds/date={df['date'].iloc[0]}/{file_name}"
            df.to_parquet(file_name, index=False)
            client.fput_object(BUCKET, s3_path, file_name)
            success = True
        except Exception as e:
            logging.error(f"[API FAIL] {sym} failed: {str(e)}")

    end_time = datetime.now(timezone.utc)
    logging.info(f"Finished bonds EOD scrape at {end_time.isoformat()} (duration: {end_time - start_time})")

    if not success:
        raise RuntimeError("No bond data fetched successfully")

if __name__ == "__main__":
    run_bonds_eod_writer()



