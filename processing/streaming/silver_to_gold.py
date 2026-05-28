import argparse
import os
import sys

# Ensure parent processing/ directory is in sys.path so we can import core/jobs
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from core.spark_session import get_spark_session
from jobs.bronze_to_postgres import sync_hive_delta_table

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Realtime / Batch ETL job to process Silver Delta tables into Gold dimension tables."
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("SILVER_BASE_PATH", "s3a://silver-lakehouse"),
        help="Silver base path on MinIO/S3.",
    )
    parser.add_argument(
        "--gold-base",
        default=os.getenv("GOLD_BASE_PATH", "s3a://gold-lakehouse"),
        help="Gold base path on MinIO/S3.",
    )
    parser.add_argument(
        "--checkpoint-base",
        default=os.getenv(
            "SILVER_TO_GOLD_CHECKPOINT_PATH",
            "s3a://gold-lakehouse/_checkpoints/silver_to_gold",
        ),
        help="Checkpoint base directory for Structured Streaming.",
    )
    parser.add_argument(
        "--hive-db",
        default=os.getenv("GOLD_HIVE_DATABASE", "gold"),
        help="Hive database name for Gold tables.",
    )
    parser.add_argument(
        "--skip-hive-sync",
        action="store_true",
        default=os.getenv("GOLD_SKIP_HIVE_SYNC", "false").lower() == "true",
        help="Skip syncing Gold tables metadata to Hive Metastore.",
    )
    parser.add_argument(
        "--processing-time",
        default=os.getenv("SILVER_TO_GOLD_PROCESSING_TIME", "10 seconds"),
        help="Streaming trigger interval, for example: 10 seconds.",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process currently available Silver data then stop. Useful for backfills/checks.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Read Silver as a batch and stop. Easier to debug than Structured Streaming.",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Optional comma-separated dimension tables allow-list (e.g. dim_products,dim_customers).",
    )
    return parser.parse_args()


def build_spark():
    spark = get_spark_session(
        app_name="SilverToGoldRealtime",
        enable_hive_support=True,
        log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
    )
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
    return spark


def read_silver(spark, silver_base, table_name):
    path = f"{silver_base.rstrip('/')}/{table_name}"
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        print(f"⚠️  Không thể đọc bảng Silver '{table_name}' tại '{path}' (Có thể bảng chưa tồn tại): {e}")
        return None


def build_dim_platforms(spark, silver_base, primary_df=None):
    platforms = primary_df if primary_df is not None else read_silver(spark, silver_base, "platforms")
    if not platforms:
        return None
    
    return platforms.select(
        "platform_id",
        "platform_code",
        "platform_name",
        "base_url",
        "is_active",
        "created_at",
        "updated_at"
    )


def build_dim_brands(spark, silver_base, primary_df=None):
    brands = primary_df if primary_df is not None else read_silver(spark, silver_base, "brands")
    platforms = read_silver(spark, silver_base, "platforms")
    if not brands:
        return None
    
    if platforms:
        joined = brands.join(platforms, "platform_id", "left")
        return joined.select(
            brands.brand_id,
            brands.platform_id,
            platforms.platform_code,
            platforms.platform_name,
            brands.platform_brand_id,
            brands.brand_name,
            brands.country,
            brands.is_official,
            brands.created_at,
            brands.updated_at
        )
    else:
        return brands.select(
            "brand_id",
            "platform_id",
            F.lit(None).cast(StringType()).alias("platform_code"),
            F.lit(None).cast(StringType()).alias("platform_name"),
            "platform_brand_id",
            "brand_name",
            "country",
            "is_official",
            "created_at",
            "updated_at"
        )


def build_dim_sellers(spark, silver_base, primary_df=None):
    sellers = primary_df if primary_df is not None else read_silver(spark, silver_base, "sellers")
    platforms = read_silver(spark, silver_base, "platforms")
    if not sellers:
        return None
    
    if platforms:
        joined = sellers.join(platforms, "platform_id", "left")
        return joined.select(
            sellers.seller_id,
            sellers.platform_id,
            platforms.platform_code,
            platforms.platform_name,
            sellers.platform_seller_id,
            sellers.seller_name,
            sellers.city,
            sellers.province,
            sellers.follower_count,
            sellers.is_official_store,
            sellers.status,
            sellers.created_at,
            sellers.updated_at
        )
    else:
        return sellers.select(
            "seller_id",
            "platform_id",
            F.lit(None).cast(StringType()).alias("platform_code"),
            F.lit(None).cast(StringType()).alias("platform_name"),
            "platform_seller_id",
            "seller_name",
            "city",
            "province",
            "follower_count",
            "is_official_store",
            "status",
            "created_at",
            "updated_at"
        )


def build_dim_customers(spark, silver_base, primary_df=None):
    customers = primary_df if primary_df is not None else read_silver(spark, silver_base, "customers")
    platforms = read_silver(spark, silver_base, "platforms")
    addresses = read_silver(spark, silver_base, "customer_addresses")
    if not customers:
        return None

    if addresses:
        window_spec = Window.partitionBy("customer_id").orderBy(
            F.col("is_default").desc(),
            F.col("updated_at").desc()
        )
        primary_addr = addresses.withColumn("rn", F.row_number().over(window_spec)) \
                                .filter(F.col("rn") == 1) \
                                .select(
                                    "customer_id",
                                    F.col("address_line").alias("primary_address_line"),
                                    F.col("ward").alias("primary_ward"),
                                    F.col("district").alias("primary_district"),
                                    F.col("city").alias("primary_city"),
                                    F.col("province").alias("primary_province"),
                                    F.col("country").alias("primary_country")
                                )
    else:
        primary_addr = None

    joined = customers
    if platforms:
        joined = joined.join(platforms, "platform_id", "left")
    else:
        joined = joined.withColumn("platform_code", F.lit(None).cast(StringType())) \
                       .withColumn("platform_name", F.lit(None).cast(StringType()))

    if primary_addr:
        joined = joined.join(primary_addr, "customer_id", "left")
    else:
        joined = joined.withColumn("primary_address_line", F.lit(None).cast(StringType())) \
                       .withColumn("primary_ward", F.lit(None).cast(StringType())) \
                       .withColumn("primary_district", F.lit(None).cast(StringType())) \
                       .withColumn("primary_city", F.lit(None).cast(StringType())) \
                       .withColumn("primary_province", F.lit(None).cast(StringType())) \
                       .withColumn("primary_country", F.lit(None).cast(StringType()))

    return joined.select(
        customers.customer_id,
        customers.platform_id,
        F.col("platform_code"),
        F.col("platform_name"),
        customers.platform_customer_id,
        customers.full_name,
        customers.email,
        customers.phone_number,
        customers.gender,
        customers.date_of_birth,
        customers.status,
        F.col("primary_address_line"),
        F.col("primary_ward"),
        F.col("primary_district"),
        F.col("primary_city"),
        F.col("primary_province"),
        F.col("primary_country"),
        customers.created_at,
        customers.updated_at
    )


def build_dim_products(spark, silver_base, primary_df=None):
    products = primary_df if primary_df is not None else read_silver(spark, silver_base, "products")
    sellers = read_silver(spark, silver_base, "sellers")
    categories = read_silver(spark, silver_base, "categories")
    brands = read_silver(spark, silver_base, "brands")
    if not products:
        return None

    joined = products
    if sellers:
        joined = joined.join(sellers.select("seller_id", "seller_name"), "seller_id", "left")
    else:
        joined = joined.withColumn("seller_name", F.lit(None).cast(StringType()))

    if categories:
        joined = joined.join(categories.select("category_id", "category_name"), "category_id", "left")
    else:
        joined = joined.withColumn("category_name", F.lit(None).cast(StringType()))

    if brands:
        joined = joined.join(brands.select("brand_id", "brand_name"), "brand_id", "left")
    else:
        joined = joined.withColumn("brand_name", F.lit(None).cast(StringType()))

    return joined.select(
        products.product_id,
        products.platform_product_id,
        products.seller_id,
        F.col("seller_name"),
        products.category_id,
        F.col("category_name"),
        products.brand_id,
        F.col("brand_name"),
        products.product_name,
        products.description,
        products.status,
        products.is_authentic,
        products.published_at,
        products.created_at,
        products.updated_at
    )


def build_dim_product_variants(spark, silver_base, primary_df=None):
    variants = primary_df if primary_df is not None else read_silver(spark, silver_base, "product_variants")
    products = read_silver(spark, silver_base, "products")
    if not variants:
        return None

    if products:
        joined = variants.join(products.select("product_id", "product_name"), "product_id", "left")
        return joined.select(
            variants.variant_id,
            variants.product_id,
            F.col("product_name"),
            variants.platform_variant_id,
            variants.sku,
            variants.variant_name,
            variants.original_price,
            variants.sale_price,
            variants.weight_gram,
            variants.status,
            variants.created_at,
            variants.updated_at
        )
    else:
        return variants.select(
            "variant_id",
            "product_id",
            F.lit(None).cast(StringType()).alias("product_name"),
            "platform_variant_id",
            "sku",
            "variant_name",
            "original_price",
            "sale_price",
            "weight_gram",
            "status",
            "created_at",
            "updated_at"
        )


BUILDERS = {
    "dim_platforms": build_dim_platforms,
    "dim_brands": build_dim_brands,
    "dim_sellers": build_dim_sellers,
    "dim_customers": build_dim_customers,
    "dim_products": build_dim_products,
    "dim_product_variants": build_dim_product_variants,
}

PRIMARY_KEYS = {
    "dim_platforms": ["platform_id"],
    "dim_brands": ["brand_id"],
    "dim_sellers": ["seller_id"],
    "dim_customers": ["customer_id"],
    "dim_products": ["product_id"],
    "dim_product_variants": ["variant_id"],
}

PRIMARY_SILVER_TABLES = {
    "dim_platforms": "platforms",
    "dim_brands": "brands",
    "dim_sellers": "sellers",
    "dim_customers": "customers",
    "dim_products": "products",
    "dim_product_variants": "product_variants",
}


def merge_gold_table(df, primary_keys, target_path):
    delta_table = DeltaTable.forPath(df.sparkSession, target_path)
    merge_condition = " AND ".join([f"target.`{key}` = source.`{key}`" for key in primary_keys])
    
    # Get all clean columns from dataframe
    columns = [col for col in df.columns if not col.startswith("_")]
    values = {column: f"source.`{column}`" for column in columns}
    
    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=values)
        .whenNotMatchedInsert(values=values)
        .execute()
    )


def write_gold_batch(df, primary_keys, target_path):
    if df.rdd.isEmpty():
        return

    df = df.persist()
    try:
        if DeltaTable.isDeltaTable(df.sparkSession, target_path):
            merge_gold_table(df, primary_keys, target_path)
        else:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
    finally:
        df.unpersist()


def process_table_batch_mode(spark, table_name, builder_func, args):
    """Fallback batch ETL mode if --once or --available-now is specified."""
    df = builder_func(spark, args.silver_base)
    if df is None:
        print(f"⚠️  Bỏ qua {table_name} vì thiếu bảng nguồn.")
        return

    target_path = f"{args.gold_base.rstrip('/')}/{table_name}"
    
    print(f"💾 Đang ghi {table_name} xuống Gold Delta Lake (Batch)...")
    write_gold_batch(df, PRIMARY_KEYS[table_name], target_path)
    print(f"✅ Đã xử lý xong {table_name} tại {target_path}")

    if args.skip_hive_sync:
        print(f"   └─ Bỏ qua Hive Metastore sync theo cấu hình.")
    else:
        print(f"🔄 Đồng bộ metadata của {table_name} vào Hive Metastore...")
        sync_hive_delta_table(spark, args.hive_db, table_name, target_path)
        print(f"✅ Đã đồng bộ Hive Metastore xong: {args.hive_db}.{table_name}")


def start_streaming_query(spark, table_name, builder_func, primary_keys, args):
    """Start Structured Streaming query for the gold dimension table."""
    primary_silver = PRIMARY_SILVER_TABLES[table_name]
    silver_path = f"{args.silver_base.rstrip('/')}/{primary_silver}"
    target_path = f"{args.gold_base.rstrip('/')}/{table_name}"
    checkpoint_path = f"{args.checkpoint_base.rstrip('/')}/{table_name}"
    
    print(f"⚡ Khởi động Realtime Stream: {primary_silver} ➔ {table_name}")
    
    try:
        stream_df = spark.readStream.format("delta").load(silver_path)
    except Exception as e:
        print(f"❌ Không thể đọc stream từ Silver '{primary_silver}' tại '{silver_path}': {e}")
        return None

    def process_micro_batch(batch_df: DataFrame, epoch_id: int):
        if batch_df.rdd.isEmpty():
            return
            
        print(f"\n[⚡ Realtime - {table_name} - Batch {epoch_id}] Đang xử lý micro-batch...")
        
        # Build gold using batch_df as the primary table
        gold_df = builder_func(spark, args.silver_base, primary_df=batch_df)
        if gold_df is None:
            return
            
        write_gold_batch(gold_df, primary_keys, target_path)
        print(f"✅ Đã cập nhật xong realtime {table_name} tại {target_path}")
        
        if not args.skip_hive_sync:
            sync_hive_delta_table(spark, args.hive_db, table_name, target_path)

    query = (
        stream_df.writeStream
        .foreachBatch(process_micro_batch)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .queryName(f"Stream_{table_name}")
    )
    
    if args.available_now:
        query = query.trigger(availableNow=True)
    else:
        query = query.trigger(processingTime=args.processing_time)
        
    return query.start()


def selected_tables(args):
    all_tables = set(BUILDERS.keys())
    if not args.tables:
        return all_tables

    tables = {item.strip() for item in args.tables.split(",") if item.strip()}
    unknown_tables = sorted(tables - all_tables)
    if unknown_tables:
        raise ValueError(
            f"Các bảng yêu cầu chưa được hỗ trợ: {', '.join(unknown_tables)}. "
            f"Hỗ trợ: {', '.join(sorted(all_tables))}"
        )
    return tables


def main():
    args = parse_args()
    target_tables = selected_tables(args)
    spark = build_spark()

    print("==================================================")
    print(f"🚀 BẮT ĐẦU CHẠY ETL SILVER -> GOLD ({'BATCH' if args.once else 'REALTIME STREAMING'})")
    print(f"📁 Silver Base Path: {args.silver_base}")
    print(f"📁 Gold Base Path:   {args.gold_base}")
    if not args.once:
        print(f"📁 Checkpoint Path:   {args.checkpoint_base}")
        print(f"⏱️  Trigger Interval: {args.processing_time}")
    print(f"🗃️ Hive Database:    {args.hive_db}")
    print(f"📋 Danh sách bảng:   {', '.join(sorted(target_tables))}")
    print("==================================================")

    # Đảm bảo database Hive cho lớp Gold tồn tại
    if not args.skip_hive_sync:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.hive_db}")

    try:
        ordered_tables = [
            t for t in ["dim_platforms", "dim_brands", "dim_sellers", "dim_customers", "dim_products", "dim_product_variants"]
            if t in target_tables
        ]
        
        if args.once:
            # Chạy chế độ Batch thông thường
            for table_name in ordered_tables:
                print(f"\n--- [Bắt đầu xử lý {table_name}] ---")
                try:
                    process_table_batch_mode(spark, table_name, BUILDERS[table_name], args)
                except Exception as e:
                    print(f"❌ Thất bại khi xử lý bảng {table_name}: {e}")
                    raise e
            print("\n==================================================")
            print("🎉 HOÀN THÀNH BATCH JOB SILVER -> GOLD THÀNH CÔNG!")
            print("==================================================")
        else:
            # Khởi động các Realtime Structured Streams song song
            active_queries = []
            for table_name in ordered_tables:
                query = start_streaming_query(
                    spark, 
                    table_name, 
                    BUILDERS[table_name], 
                    PRIMARY_KEYS[table_name], 
                    args
                )
                if query:
                    active_queries.append(query)
            
            if active_queries:
                print(f"\n🚀 Đang chạy {len(active_queries)} Realtime Streams song song...")
                # Đợi cho các stream kết thúc
                spark.streams.awaitAnyTermination()
            else:
                print("⚠️  Không có stream nào được khởi động.")
                
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"❌ ETL Realtime Silver -> Gold thất bại: {exc}")
        sys.exit(1)
