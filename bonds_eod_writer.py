from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf
from .config import MINIO_BUCKET, BONDS
from .minio_utils import get_minio_client, ensure_bucket, put_parquet

def fetch_eod_bonds(symbols):
    rows = []
    for s in symbols:
        df = yf.download(s, period="5d", interval="1d", progress=False)
        if df.empty:
            continue
        df = df.tail(1).reset_index().rename(columns={"Date": "date"})
        for _, r in df.iterrows():
            rows.append({
                "symbol": s,
                "date": r["date"].date().isoformat(),
                "close": float(r["Close"]),
            })
    return pd.DataFrame(rows)

def run_bonds_eod_writer():
    df = fetch_eod_bonds(BONDS)
    if df.empty:
        print("[bonds_eod_writer] no data")
        return
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)
    for _, row in df.iterrows():
        key = f"eod/bonds/date={row['date']}/symbol={row['symbol']}.parquet"
        table = pa.Table.from_pydict({
            "symbol": [row["symbol"]],
            "date": [row["date"]],
            "close": [row["close"]],
        })
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf, compression="snappy")
        put_parquet(client, MINIO_BUCKET, key, buf.getvalue().to_pybytes())
    print(f"[bonds_eod_writer] wrote {len(df)} files to s3://{MINIO_BUCKET}/eod/bonds/")
