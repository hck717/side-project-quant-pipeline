import yfinance as yf
import json
import logging
from confluent_kafka import Producer
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_fixed

# Configure logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

KAFKA_BROKER = "redpanda:9092"
TOPIC = "equities_ticks"
EQUITY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_intraday_data(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="1d", interval="5m")
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df.iloc[-1:]  # Return only the latest row

def delivery_report(err, msg):
    """Kafka delivery callback."""
    if err is not None:
        logging.error(f"Kafka delivery failed for {msg.key()}: {err}")
    else:
        logging.info(
            f"Produced to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}"
        )

def run_equities_intraday_producer():
    start_time = datetime.now(timezone.utc)
    logging.info(f"Starting equities intraday scrape at {start_time.isoformat()}")

    p = Producer({'bootstrap.servers': KAFKA_BROKER})
    success = False

    for sym in EQUITY_SYMBOLS:
        try:
            df = fetch_intraday_data(sym)
            latest_row = df.iloc[-1]
            msg = {
                "symbol": sym,
                "timestamp": latest_row.name.isoformat(),
                "open": float(latest_row["Open"]),
                "high": float(latest_row["High"]),
                "low": float(latest_row["Low"]),
                "close": float(latest_row["Close"]),
                "volume": float(latest_row["Volume"]),
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }
            logging.info(f"[API OK] {sym} latest data: {msg}")
            p.produce(TOPIC, json.dumps(msg).encode('utf-8'), callback=delivery_report)
            success = True
        except Exception as e:
            logging.error(f"[API FAIL] {sym} failed: {str(e)}")

    p.flush()
    end_time = datetime.now(timezone.utc)
    logging.info(f"Finished equities intraday scrape at {end_time.isoformat()} "
                 f"(duration: {end_time - start_time})")

    if not success:
        raise RuntimeError("No equity intraday data fetched successfully")

if __name__ == "__main__":
    run_equities_intraday_producer()
