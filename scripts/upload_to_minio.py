import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123",
)

try:
    s3.upload_file(
        Filename="raw_data/stock_prices/backfill_3y.parquet",
        Bucket="raw-data",
        Key="stock_prices/backfill_3y.parquet",
    )
    print("Upload done.")
except Exception as e:
    print(f"Error: {e}")

