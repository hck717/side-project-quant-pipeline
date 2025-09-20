import yfinance as yf
import json
import logging
from confluent_kafka import Producer
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_fixed

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)

KAFKA_BROKER = "redpanda:9092"
TOPIC = "crypto_ticks"
CRYPTO_PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD"]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_crypto_data(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="1d", interval="1m")
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df.iloc[-1:]

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Kafka delivery failed for {msg.key()}: {err}")
    else:
        logging.info(f"Produced to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")

def run_crypto_tick_producer():
    start_time = datetime.now(timezone.utc)
    logging.info(f"Starting crypto scrape at {start_time.isoformat()}")

    p = Producer({'bootstrap.servers': KAFKA_BROKER})
    success = False

    for symbol in CRYPTO_PAIRS:
        try:
            df = fetch_crypto_data(symbol)
            latest_row = df.iloc[-1]
            msg = {
                "symbol": symbol,
                "timestamp": latest_row.name.isoformat(),
                "open": float(latest_row["Open"]),
                "high": float(latest_row["High"]),
                "low": float(latest_row["Low"]),
                "close": float(latest_row["Close"]),
                "volume": float(latest_row["Volume"]),
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }
            logging.info(f"[API OK] {symbol} latest data: {msg}")
            p.produce(TOPIC, json.dumps(msg).encode('utf-8'), callback=delivery_report)
            success = True
        except Exception as e:
            logging.error(f"[API FAIL] {symbol} failed: {str(e)}")

    p.flush()
    end_time = datetime.now(timezone.utc)
    logging.info(f"Finished crypto scrape at {end_time.isoformat()} (duration: {end_time - start_time})")

    if not success:
        raise RuntimeError("No crypto data fetched successfully")

if __name__ == "__main__":
    run_crypto_tick_producer()
