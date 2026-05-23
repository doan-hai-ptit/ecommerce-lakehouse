import os
import sys
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Ensure parent processing/ directory is in sys.path so we can import core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.spark_session import get_spark_session

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
print(endpoint_url)

# Khởi tạo Spark Session dùng chung
spark = get_spark_session(
    app_name=f"BronzeToSilver_{source.upper()}",
    log_level=os.getenv("SPARK_LOG_LEVEL", "WARN")
)

# Định nghĩa Dynamic Paths dựa theo nguồn dữ liệu
bronze_path = f"s3a://bronze-lakehouse/provider={source}/date=*/category=products/*.json"
silver_path = f"s3a://silver-lakehouse/ecom_products/platform={source}"

print(f"⏳ Bắt đầu đọc dữ liệu thô từ: {bronze_path}")

# Đọc dữ liệu Bronze (JSON đa dòng)
df_raw = spark.read.option("multiline", "true").json(bronze_path)

# Khởi tạo Transformation & Chuẩn hóa Schema theo mẫu chung
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

# Ghi dữ liệu vào Delta Lake lớp Silver
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
