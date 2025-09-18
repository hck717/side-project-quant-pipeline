import os
import time
import requests
import json
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer

KAFKA_BROKER = "redpanda:9092"
TOPIC = "equities.intraday"
EQUITY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

def run_equities_intraday_producer():
    p = Producer({'bootstrap.servers': KAFKA_BROKER})
    for sym in EQUITY_SYMBOLS:
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": sym,
            "interval": "5min",
            "outputsize": "compact",
            "apikey": API_KEY
        }
        r = requests.get(BASE_URL, params=params)
        r.raise_for_status()
        data = r.json()
        ts_key = "Time Series (5min)"
        if ts_key not in data:
            continue
        df = pd.DataFrame.from_dict(data[ts_key], orient="index").astype(float)
        df.index = pd.to_datetime(df.index)
        latest_ts = df.index.max()
        latest_row = df.loc[[latest_ts]]
        # 👇 Debug print for Airflow logs
        print(f"[SCRAPE DEBUG] {sym} latest data: {latest_row.to_dict(orient='records')}")

        for idx, row in latest_row.iterrows():
            msg = {
                "symbol": sym,
                "timestamp": idx.isoformat(),
                "open": row["1. open"],
                "high": row["2. high"],
                "low": row["3. low"],
                "close": row["4. close"],
                "volume": row["5. volume"],
                "ingested_at": datetime.utcnow().isoformat()
            }
            p.produce(TOPIC, json.dumps(msg).encode('utf-8'))
        time.sleep(12)
    p.flush()

