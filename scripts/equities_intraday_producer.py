import yfinance as yf
import json
from confluent_kafka import Producer
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_fixed

KAFKA_BROKER = "redpanda:9092"
TOPIC = "equities_ticks"  # updated from "equities.intraday"
EQUITY_SYMBOLS = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_intraday_data(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="1d", interval="5m")
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df.iloc[-1:]  # Return only the latest row

def run_equities_intraday_producer():
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
            print(f"[SCRAPE DEBUG] {sym} latest data: {msg}")
            p.produce(TOPIC, json.dumps(msg).encode('utf-8'))
            success = True
        except Exception as e:
            print(f"[SCRAPE ERROR] {sym} failed: {str(e)}")
    p.flush()
    if not success:
        raise RuntimeError("No equity intraday data fetched successfully")
