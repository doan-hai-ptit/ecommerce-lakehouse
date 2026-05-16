from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import os

# Khởi tạo Spark Session với cấu hình Delta và Hive
spark = SparkSession.builder \
    .appName("Tiki_Lakehouse_Final_Test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    print("--- 1. Kiểm tra kết nối Hive & Postgres ---")
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql("USE test_db")
    print("✔ Đã tạo/Kết nối Database thành công!")

    print("\n--- 2. Tạo dữ liệu mẫu ---")
    data = [
        (1242, "Nam Việt Biotech", "https://tiki.vn/cua-hang/1242"),
        (1, "Tiki Trading", "https://tiki.vn/cua-hang/tiki-trading")
    ]
    columns = ["store_id", "store_name", "url"]
    df = spark.createDataFrame(data, columns).withColumn("processed_at", current_timestamp())

    print("\n--- 3. Ghi dữ liệu Delta Lake vào MinIO ---")
    # Đường dẫn lưu trên MinIO
    target_path = "s3a://silver-lakehouse/test_table"
    
    # Ghi đồng thời vào MetaStore (Postgres) và Dữ liệu (MinIO)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", "s3a://silver-lakehouse/test_table") \
        .save()
    print(f"✔ Đã lưu bảng Delta vào MinIO tại: {target_path}")

    print("\n--- 4. Truy vấn lại bằng SQL (Hive Metastore) ---")
        # Chọn database làm việc
    spark.sql("USE test_db")

    # SỬA TẠI ĐÂY: Đăng ký bảng ngoại vi (External Table) trỏ đến thư mục Delta vừa ghi trên MinIO
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS test_table 
        USING DELTA 
        LOCATION '{target_path}'
    """)

    # Tiến hành truy vấn dữ liệu bằng SQL bình thường
    result_df = spark.sql("SELECT * FROM test_table WHERE store_id = 1242")
    result_df.show()

    print("✔ Đã truy vấn thành công dữ liệu từ Hive Metastore!")

    print("\n--- 5. Kiểm tra lịch sử bảng (Delta Time Travel) ---")
    spark.sql("DESCRIBE HISTORY test_table").select("version", "timestamp", "operation").show()

    print("\n🚀 CHÚC MỪNG! HỆ THỐNG LAKEHOUSE ĐÃ CHẠY HOÀN HẢO!")

except Exception as e:
    print(f"\n❌ LỖI: {e}")

finally:
    spark.stop()