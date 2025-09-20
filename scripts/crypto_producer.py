import yfinance as yf
import json
from confluent_kafka import Producer
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_fixed

KAFKA_BROKER = "redpanda:9092"
TOPIC = "crypto_ticks"  # updated from "crypto.ticks"
CRYPTO_PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD"]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_crypto_data(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="1d", interval="1m")
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df.iloc[-1:]  # Return only the latest row

def run_crypto_tick_producer():
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
            print(f"[SCRAPE DEBUG] {symbol} latest data: {msg}")
            p.produce(TOPIC, json.dumps(msg).encode('utf-8'))
            success = True
        except Exception as e:
            print(f"[SCRAPE ERROR] {symbol} failed: {str(e)}")
    p.flush()
    if not success:
        raise RuntimeError("No crypto data fetched successfully")
