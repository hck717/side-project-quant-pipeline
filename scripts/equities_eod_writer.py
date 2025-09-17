import yfinance as yf
from minio import Minio

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "quant"
EQUITY_SYMBOLS = ["AAPL", "MSFT", "GOOG"]

def run_equities_eod_writer():
    df = yf.download(EQUITY_SYMBOLS, period="1mo", interval="1d", group_by='ticker', threads=True)
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    for sym in EQUITY_SYMBOLS:
        try:
            sym_df = df[sym].reset_index().dropna(subset=["Date"])
            latest_date = sym_df["Date"].max().date()
            latest_rows = sym_df[sym_df["Date"].dt.date == latest_date]
            file_name = f"{sym}.parquet"
            latest_rows.to_parquet(file_name, index=False)
            s3_path = f"eod/equities/date={latest_date}/{file_name}"
            client.fput_object(BUCKET, s3_path, file_name)
        except KeyError:
            pass
