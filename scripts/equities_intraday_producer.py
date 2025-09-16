import yfinance as yf
import pandas as pd
import json
import logging
from confluent_kafka import Producer

EQUITY_SYMBOLS = ["AAPL", "MSFT", "GOOG"]
KAFKA_BROKER = "redpanda:9092"
TOPIC = "equities.intraday"

def run_equities_intraday_producer():
    logging.info("[equities_intraday_producer] Fetching equities intraday data...")
    df = yf.download(EQUITY_SYMBOLS, period="5d", interval="1m", group_by='ticker', threads=True)
    if df.empty:
        logging.warning("[equities_intraday_producer] No data fetched.")
        return

    records = []
    for sym in EQUITY_SYMBOLS:
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
            logging.warning(f"[equities_intraday_producer] No data for {sym}")

    if records:
        try:
            p = Producer({'bootstrap.servers': KAFKA_BROKER})
            for rec in records:
                p.produce(TOPIC, json.dumps(rec).encode('utf-8'))
            p.flush()
            logging.info(f"[equities_intraday_producer] Produced {len(records)} messages to {TOPIC}")
        except Exception as e:
            logging.error(f"[equities_intraday_producer] Kafka error: {e}")
    else:
        logging.warning("[equities_intraday_producer] No records to produce.")

if __name__ == "__main__":
    run_equities_intraday_producer()
