import yfinance as yf
import json
from confluent_kafka import Producer
from datetime import datetime

KAFKA_BROKER = "redpanda:9092"
TOPIC = "crypto.ticks"
CRYPTO_SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD"]

def run_crypto_tick_producer():
    df = yf.download(CRYPTO_SYMBOLS, period="1d", interval="1m", group_by='ticker', threads=True)
    p = Producer({'bootstrap.servers': KAFKA_BROKER})

    for sym in CRYPTO_SYMBOLS:
        try:
            sym_df = df[sym].reset_index().dropna(subset=["Datetime"])
            latest_ts = sym_df["Datetime"].max()
            latest_rows = sym_df[sym_df["Datetime"] == latest_ts]
            for _, row in latest_rows.iterrows():
                msg = {
                    "symbol": sym,
                    "timestamp": row["Datetime"].isoformat(),
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "volume": row["Volume"],
                    "ingested_at": datetime.utcnow().isoformat()
                }
                p.produce(TOPIC, json.dumps(msg).encode('utf-8'))
        except KeyError:
            pass

    p.flush()

