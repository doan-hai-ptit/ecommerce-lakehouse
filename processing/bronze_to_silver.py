import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

load_dotenv()

# Kiểm tra tham số truyền vào (Ví dụ: python bronze_to_silver.py tiki)
if len(sys.argv) < 2:
    print("❌ Lỗi: Thiếu tham số nguồn dữ liệu. Cú pháp: python bronze_to_silver.py [tiki|sendo|shopee]")
    sys.exit(1)

source = sys.argv[1].lower()
if source not in ["tiki", "sendo", "shopee"]:
    print(f"❌ Lỗi: Nguồn '{source}' không được hỗ trợ. Chỉ chấp nhận 'tiki', 'sendo' hoặc 'shopee'.")
    sys.exit(1)

# Lấy cấu hình MinIO từ file .env
endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
print(endpoint_url)
# 1. Khởi tạo Builder cho Spark & Delta
builder = SparkSession.builder.appName(f"BronzeToSilver_{source.upper()}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("spark.sql.jsonGenerator.ignoreNullFields", "false")

# 2. Khởi tạo Spark Session
spark = builder.getOrCreate()
# 3. Định nghĩa Dynamic Paths dựa theo nguồn dữ liệu
bronze_path = f"s3a://bronze-lakehouse/provider={source}/date=*/category=products/*.json"
silver_path = f"s3a://silver-lakehouse/ecom_products/platform={source}"

print(f"⏳ Bắt đầu đọc dữ liệu thô từ: {bronze_path}")

# 4. Đọc dữ liệu Bronze (JSON đa dòng)
df_raw = spark.read.option("multiline", "true").json(bronze_path)
# 5. Khởi tạo Transformation & Chuẩn hóa Schema theo mẫu chung
if source in ["tiki", "shopee"]:
    df_silver = df_raw.select(
        F.col("id").cast(StringType()).alias("product_id"),
        F.col("name").alias("product_name"),
        F.col("sku").alias("sku"),
        F.col("original_price").cast("long").alias("original_price"),
        F.col("price").cast("long").alias("final_price"),
        # Xử lý trường hợp quantity_sold là một Object chứa trường value (ví dụ: {"value": 120, ...})
        F.expr("CASE WHEN isnotnull(quantity_sold.value) THEN quantity_sold.value ELSE 0 END").cast("int").alias("sold_count"),
        F.col("thumbnail_url").alias("image_url"),
        F.col("brand_name").alias("brand"),
        # Gom các trường đặc trưng của Tiki vào struct riêng nếu cần giữ lại
        F.struct(
            F.col("url_key"), F.col("url_path"), F.col("seller_name"), F.col("product_rating")
        ).alias("metadata")
    )
else:
    df_silver = df_raw.select(
        F.col("product_id").cast(StringType()).alias("product_id"),
        F.col("product_name").alias("product_name"),
        F.col("product_sku").alias("sku"),
        F.col("price").cast("long").alias("original_price"),
        F.col("final_price").cast("long").alias("final_price"),
        # Sendo lưu sẵn order_count dạng số phẳng
        F.coalesce(F.col("order_count"), F.lit(0)).cast("int").alias("sold_count"),
        F.col("image").alias("image_url"),
        # Sendo không có brand rõ ràng ở tầng ngoài, lấy tạm trường nhãn hoặc mặc định "Unknown"
        F.coalesce(F.col("tag_name"), F.lit("Unknown")).alias("brand"),
        # Gom các trường đặc trưng của Sendo (Sendofarm)
        F.struct(
            F.col("category_id"), F.col("quantity"), F.col("label_order_count"), F.col("pack_group_info")
        ).alias("metadata")
    )

# Thêm cột audit tracking thời gian xử lý lớp Silver
if df_silver is not None:
    df_silver = df_silver.withColumn("ingested_at", F.current_timestamp())

# 6. Ghi dữ liệu vào Delta Lake lớp Silver
print(f"🚀 Đang ghi dữ liệu chuẩn hóa Delta Lake vào: {silver_path}")

# Sử dụng overwrite nếu muốn làm sạch mỗi lần chạy batch toàn bộ, 
# hoặc chuyển sang .mode("append") nếu bạn xử lý incremental (tăng trưởng) theo ngày
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(silver_path)

print(f"✔ [Thành công] Đã xử lý {source.upper()} từ Bronze sang Silver tại: {silver_path}")

spark.stop()
