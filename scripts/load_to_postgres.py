import boto3
import pandas as pd
from sqlalchemy import create_engine

# Download from MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123",
)

s3.download_file(
    Bucket="raw-data",
    Key="stock_prices/backfill_3y.parquet",
    Filename="temp_backfill.parquet",
)

# Read parquet
df = pd.read_parquet("temp_backfill.parquet")
print(f"Rows: {len(df)}, Columns: {list(df.columns)}")

# Load into Postgres
engine = create_engine("postgresql://warehouse:warehouse123@localhost:5432/stock_warehouse")
df.to_sql("raw_stock_prices", engine, if_exists="replace", index=False)

print("Loaded into Postgres table: raw_stock_prices")