import yfinance as yf
import pandas as pd
from minio import Minio
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed

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
    return df.iloc[-1:]  # Return only the latest row

def run_bonds_eod_writer():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)
    for sym in BOND_SYMBOLS:
        try:
            df = fetch_bond_data(sym)
            df = df.reset_index().rename(columns={"Date": "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.date
            latest_date = df["date"].max()
            print(f"[SCRAPE DEBUG] {sym} latest data: {df.to_dict(orient='records')}")

            file_name = f"{sym}.parquet"
            df.to_parquet(file_name, index=False)
            s3_path = f"eod/bonds/date={latest_date}/{file_name}"
            client.fput_object(BUCKET, s3_path, file_name)
        except Exception as e:
            print(f"[SCRAPE ERROR] {sym} failed: {str(e)}")