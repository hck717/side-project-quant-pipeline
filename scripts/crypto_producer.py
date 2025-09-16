import yfinance as yf
import pandas as pd
import json
import logging
from confluent_kafka import Producer

CRYPTO_SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD"]
KAFKA_BROKER = "redpanda:9092"
TOPIC = "crypto.ticks"

def run_crypto_tick_producer():
    logging.info("[crypto_producer] Fetching crypto data...")
    df = yf.download(CRYPTO_SYMBOLS, period="5d", interval="1m", group_by='ticker', threads=True)
    if df.empty:
        logging.warning("[crypto_producer] No data fetched.")
        return

    records = []
    for sym in CRYPTO_SYMBOLS:
        try:
            sym_df = df[sym].reset_index().dropna(subset=["Datetime"])
            if sym_df.empty:
                continue
            latest_ts = sym_df["Datetime"].max()
            latest_rows = sym_df[sym_df["Datetime"] == latest_ts]
            for _, row in latest_rows.iterrows():
                records.append({
                    "symbol": sym,
                    "timestamp": row["Datetime"].isoformat(),
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "volume": row["Volume"]
                })
        except KeyError:
            logging.warning(f"[crypto_producer] No data for {sym}")

    if records:
        try:
            p = Producer({'bootstrap.servers': KAFKA_BROKER})
            for rec in records:
                p.produce(TOPIC, json.dumps(rec).encode('utf-8'))
            p.flush()
            logging.info(f"[crypto_producer] Produced {len(records)} messages to {TOPIC}")
        except Exception as e:
            logging.error(f"[crypto_producer] Kafka error: {e}")
    else:
        logging.warning("[crypto_producer] No records to produce.")

if __name__ == "__main__":
    run_crypto_tick_producer()

