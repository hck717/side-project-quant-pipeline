import yfinance as yf
import pandas as pd
from minio import Minio
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_fixed

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "quant"

# Primary bond yield symbols and ETF proxies
BOND_SYMBOLS = ["^TNX", "^IRX", "^FVX", "^TYX"]
BOND_FALLBACKS = {
    "^TNX": "IEF",  # 7-10 Year Treasury ETF
    "^IRX": "BIL",  # 1-3 Month Treasury ETF
    "^FVX": "SHY",  # 1-3 Year Treasury ETF
    "^TYX": "TLT",  # 20+ Year Treasury ETF
}

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_bond_data(symbol, period="1d", interval="1d"):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=period, interval=interval)
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df.iloc[-1:]  # Return only the latest row

def run_bonds_eod_writer():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)

    # Ensure bucket exists
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    success = False
    for sym in BOND_SYMBOLS:
        try:
            df = fetch_bond_data(sym, period="1d", interval="1d")
        except Exception as e:
            print(f"[SCRAPE WARN] {sym} failed: {e}")
            fallback = BOND_FALLBACKS.get(sym)
            if fallback:
                print(f"[SCRAPE WARN] Trying fallback symbol {fallback} for {sym}")
                try:
                    df = fetch_bond_data(fallback, period="1d", interval="1d")
                except Exception as e2:
                    print(f"[SCRAPE ERROR] Fallback {fallback} also failed: {e2}")
                    continue
            else:
                continue

        # If we got here, df has data
        df = df.reset_index().rename(columns={"Date": "date"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["ingested_at"] = datetime.now(timezone.utc).isoformat()
        print(f"[SCRAPE DEBUG] {sym} latest data: {df.to_dict(orient='records')}")
        file_name = f"{sym.replace('^','')}.parquet"
        s3_path = f"eod/bonds/date={df['date'].iloc[0]}/{file_name}"
        df.to_parquet(file_name, index=False)
        client.fput_object(BUCKET, s3_path, file_name)
        success = True

    if not success:
        raise RuntimeError("No bond data fetched successfully")

if __name__ == "__main__":
    run_bonds_eod_writer()
