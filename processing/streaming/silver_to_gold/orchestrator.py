import os
import sys
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from core.spark_session import get_spark_session
from jobs.bronze_to_postgres import sync_hive_delta_table
from .builders import BUILDERS, PRIMARY_KEYS, PRIMARY_SILVER_TABLES
from .config import parse_args, selected_tables


def build_spark():
    spark = get_spark_session(
        app_name="SilverToGoldRealtime",
        enable_hive_support=True,
        log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
    )
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
    return spark


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
