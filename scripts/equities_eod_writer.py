import os
import time
import requests
import pandas as pd
from minio import Minio

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "quant"
EQUITY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA", "JPM", "XOM", "META", "GOOGL", "NFLX"]

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

def run_equities_eod_writer():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)

    for sym in EQUITY_SYMBOLS:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": sym,
            "outputsize": "compact",
            "apikey": API_KEY
        }
        r = requests.get(BASE_URL, params=params)
        r.raise_for_status()
        data = r.json()
        ts_key = "Time Series (Daily)"
        if ts_key not in data:
            continue
        df = pd.DataFrame.from_dict(data[ts_key], orient="index").astype(float)
        df.index = pd.to_datetime(df.index)
        df = df.reset_index().rename(columns={"index": "Date"})
        latest_date = df["Date"].max().date()
        latest_rows = df[df["Date"].dt.date == latest_date]
        # 👇 Debug print for Airflow logs
        print(f"[SCRAPE DEBUG] {sym} latest data: {latest_rows.to_dict(orient='records')}")

        file_name = f"{sym}.parquet"
        latest_rows.to_parquet(file_name, index=False)
        s3_path = f"eod/equities/date={latest_date}/{file_name}"
        client.fput_object(BUCKET, s3_path, file_name)
        time.sleep(12)

