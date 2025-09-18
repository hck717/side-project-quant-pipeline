import requests
import json
from confluent_kafka import Producer
from datetime import datetime

KAFKA_BROKER = "redpanda:9092"
TOPIC = "crypto.ticks"
CRYPTO_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT"]

def run_crypto_tick_producer():
    p = Producer({'bootstrap.servers': KAFKA_BROKER})
    for symbol in CRYPTO_PAIRS:
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": symbol, "interval": "1m", "limit": 1}
        r = requests.get(url, params=params)
        if r.status_code != 200:
            continue
        kline = r.json()[0]
        msg = {
            "symbol": symbol,
            "timestamp": datetime.utcfromtimestamp(kline[0] / 1000).isoformat(),
            "open": float(kline[1]),
            "high": float(kline[2]),
            "low": float(kline[3]),
            "close": float(kline[4]),
            "volume": float(kline[5]),
            "ingested_at": datetime.utcnow().isoformat()
        }
        # 👇 Debug print for Airflow logs
        print(f"[SCRAPE DEBUG] {symbol} latest data: {msg}")

        p.produce(TOPIC, json.dumps(msg).encode('utf-8'))
    p.flush()
