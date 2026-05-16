import boto3
import json
from dotenv import load_dotenv
import os
load_dotenv()
endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("MINIO_BUCKET_NAME", "bronze-lakehouse")
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

bucket_name = "bronze-lakehouse"

key = "provider=tiki/date=2026-05-15/category=sellers/seller_15937_1778837055.json"

obj = s3.get_object(
    Bucket=bucket_name,
    Key=key
)

data = json.loads(
    obj["Body"].read().decode("utf-8")
)

print(data)