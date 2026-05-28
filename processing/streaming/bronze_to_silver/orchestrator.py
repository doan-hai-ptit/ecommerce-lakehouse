import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from core.spark_session import get_spark_session
from .schemas import TABLE_SPECS
from .config import parse_args, selected_tables
from .writer import process_table


def build_spark():
    spark = get_spark_session(
        app_name="BronzeToSilverStreaming",
        enable_hive_support=True,
        log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
    )
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
    return spark


def process_batch(batch_df: DataFrame, epoch_id: int, args, allowed_tables):
    if batch_df.rdd.isEmpty():
        return

    source_tables = {
        row["source_table"]
        for row in batch_df.select("source_table")
        .where(F.col("source_table").isNotNull())
        .distinct()
        .collect()
    }
    ordered_tables = [
        table_name
        for table_name in TABLE_SPECS
        if table_name in source_tables and table_name in allowed_tables
    ]
    skipped_tables = sorted(source_tables - allowed_tables)

    print(f"\nBatch {epoch_id}: nhận {len(source_tables)} bảng từ Bronze, xử lý {len(ordered_tables)} bảng")
    if skipped_tables:
        print(f"  - Bỏ qua theo allow-list: {', '.join(skipped_tables)}")

    for table_name in ordered_tables:
        spec = TABLE_SPECS[table_name]
        try:
            process_table(batch_df, table_name, spec, args)
        except Exception as exc:
            print(f"❌ Lỗi tại bảng {table_name}: {exc}")
            raise


def read_bronze_stream(spark, bronze_path):
    return (
        spark.readStream.format("delta")
        .load(bronze_path)
        .where(F.col("source_table").isNotNull())
        .where(F.col("payload_after").isNotNull() | F.col("payload_before").isNotNull())
    )


def read_bronze_batch(spark, bronze_path):
    return (
        spark.read.format("delta")
        .load(bronze_path)
        .where(F.col("source_table").isNotNull())
        .where(F.col("payload_after").isNotNull() | F.col("payload_before").isNotNull())
    )


def filter_allowed_tables(events_df, allowed_tables):
    return events_df.where(F.col("source_table").isin(*sorted(allowed_tables)))


def start_query(events_df, args, allowed_tables):
    writer = (
        events_df.writeStream.foreachBatch(
            lambda df, epoch_id: process_batch(df, epoch_id, args, allowed_tables)
        )
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .queryName("BronzeToSilverStreaming")
    )

    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.processing_time)

    print(f"Bronze Delta path: {args.bronze_path}")
    print(f"Silver base path: {args.silver_base}")
    print(f"Hive database: {args.hive_db}")
    print(f"Hive sync: {'off' if args.skip_hive_sync else 'on'}")
    print(f"Checkpoint path: {args.checkpoint_path}")

    return writer.start()


def main():
    args = parse_args()
    allowed_tables = selected_tables(args)
    spark = build_spark()

    try:
        if args.once:
            bronze_df = filter_allowed_tables(read_bronze_batch(spark, args.bronze_path), allowed_tables)
            process_batch(bronze_df, 0, args, allowed_tables)
        else:
            bronze_df = filter_allowed_tables(read_bronze_stream(spark, args.bronze_path), allowed_tables)
            query = start_query(bronze_df, args, allowed_tables)
            query.awaitTermination()
    finally:
        spark.stop()
