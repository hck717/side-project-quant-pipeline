import os
import time
import requests
import pandas as pd
from minio import Minio

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "quant"
BOND_SYMBOLS = {
    "^TNX": "10year",
    "^IRX": "3month",
    "^FVX": "5year",
    "^TYX": "30year"
}

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

def run_bonds_eod_writer():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)

    for sym, maturity in BOND_SYMBOLS.items():
        params = {
            "function": "TREASURY_YIELD",
            "interval": "daily",
            "maturity": maturity,
            "apikey": API_KEY
        }
        r = requests.get(BASE_URL, params=params)
        r.raise_for_status()
        data = r.json()
        if "data" not in data:
            continue
        df = pd.DataFrame(data["data"])
        df["date"] = pd.to_datetime(df["date"])
        latest_date = df["date"].max().date()
        latest_rows = df[df["date"].dt.date == latest_date]
        # 👇 Debug print for Airflow logs
        print(f"[SCRAPE DEBUG] {sym} latest data: {latest_rows.to_dict(orient='records')}")

        file_name = f"{sym}.parquet"
        latest_rows.to_parquet(file_name, index=False)
        s3_path = f"eod/bonds/date={latest_date}/{file_name}"
        client.fput_object(BUCKET, s3_path, file_name)
        time.sleep(12)
