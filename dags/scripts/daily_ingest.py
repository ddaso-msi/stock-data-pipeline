"""
Daily ingestion: Pull today's stock data, upload to MinIO, load into Postgres.
"""

import yfinance as yf
import pandas as pd
import boto3
from sqlalchemy import create_engine
from datetime import datetime

TICKERS = {
    "TCS.NS": "IT", "INFY.NS": "IT", "WIPRO.NS": "IT", "HCLTECH.NS": "IT",
    "HDFCBANK.NS": "Banking", "ICICIBANK.NS": "Banking", "SBIN.NS": "Banking", "KOTAKBANK.NS": "Banking",
    "SUNPHARMA.NS": "Pharma", "DRREDDY.NS": "Pharma", "CIPLA.NS": "Pharma", "GRANULES.NS": "Pharma",
    "HINDUNILVR.NS": "FMCG", "ITC.NS": "FMCG", "NESTLEIND.NS": "FMCG",
    "MARUTI.NS": "Auto", "TATAMOTORS.NS": "Auto", "M&M.NS": "Auto",
    "RELIANCE.NS": "Conglomerate", "LT.NS": "Infra", "ADANIENT.NS": "Infra",
    "BHARTIARTL.NS": "Telecom", "^NSEI": "Index",
}

def run():
    today = datetime.now().strftime("%Y-%m-%d")
    all_data = []

    for ticker, sector in TICKERS.items():
        try:
            df = yf.download(ticker, period="1d", progress=False)
            if df.empty:
                print(f"  SKIP {ticker}: no data")
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.reset_index()
            df["ticker"] = ticker
            df["sector"] = sector
            df["ingested_at"] = datetime.utcnow().isoformat()
            all_data.append(df)
            print(f"  OK {ticker}: {len(df)} rows")
        except Exception as e:
            print(f"  ERROR {ticker}: {e}")

    if not all_data:
        print("No data fetched. Market may be closed.")
        return

    combined = pd.concat(all_data, ignore_index=True)

    # Save to MinIO
    filename = f"/tmp/stock_prices_{today}.parquet"
    combined.to_parquet(filename, index=False)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="admin123",
    )
    s3.upload_file(filename, "raw-data", f"stock_prices/daily/{today}.parquet")
    print(f"  Uploaded to MinIO: stock_prices/daily/{today}.parquet")

    # Append to Postgres
    engine = create_engine("postgresql://warehouse:warehouse123@postgres:5432/stock_warehouse")
    combined.to_sql("raw_stock_prices", engine, if_exists="append", index=False)
    print(f"  Appended {len(combined)} rows to Postgres")

if __name__ == "__main__":
    run()