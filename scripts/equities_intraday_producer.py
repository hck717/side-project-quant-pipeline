import yfinance as yf
import json
from confluent_kafka import Producer
from datetime import datetime

KAFKA_BROKER = "redpanda:9092"
TOPIC = "equities.intraday"
EQUITY_SYMBOLS = ["AAPL", "MSFT", "GOOG"]

def run_equities_intraday_producer():
    df = yf.download(EQUITY_SYMBOLS, period="5d", interval="5m", group_by='ticker', threads=True)
    p = Producer({'bootstrap.servers': KAFKA_BROKER})

    for sym in EQUITY_SYMBOLS:
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
