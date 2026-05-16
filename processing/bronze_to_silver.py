from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
import os
load_dotenv()
endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("MINIO_BUCKET_NAME", "bronze-lakehouse")
# 1. Khởi tạo Builder
builder = SparkSession.builder.appName("TikiDeltaProcessing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")

# 2. Khởi tạo Spark Session
spark = builder.get_or_create()

# 3. Đọc dữ liệu Bronze (JSON)
# Spark 3.5 đọc JSON rất nhanh và hỗ trợ tốt các ký tự đặc biệt
df = spark.read.json("s3a://bronze-lakehouse/provider=tiki/date=*/category=sellers/*.json")

# 4. Ghi dữ liệu vào Delta Lake (Silver)
silver_path = "s3a://silver-lakehouse/tiki/sellers_delta"
df.write.format("delta").mode("overwrite").save(silver_path)

print(f"✔ Đã ghi bảng Delta thành công tại Java 17 & Spark 3.5: {silver_path}")
spark.stop()