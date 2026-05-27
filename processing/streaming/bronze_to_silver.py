import argparse
import os
import re
import sys
from dataclasses import dataclass

# Ensure parent processing/ directory is in sys.path so we can import core
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


@dataclass(frozen=True)
class TableSpec:
    columns: list[tuple[str, object]]
    primary_keys: list[str]


# Mirrors database/init_schema.sql. Silver keeps the operational table columns
# plus event_date as the Delta partition column.
TABLE_SPECS = {
    "platforms": TableSpec(
        [
            ("platform_id", IntegerType()),
            ("platform_code", StringType()),
            ("platform_name", StringType()),
            ("base_url", StringType()),
            ("is_active", BooleanType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["platform_id"],
    ),
    "sellers": TableSpec(
        [
            ("seller_id", LongType()),
            ("platform_id", IntegerType()),
            ("platform_seller_id", StringType()),
            ("seller_name", StringType()),
            ("city", StringType()),
            ("province", StringType()),
            ("follower_count", IntegerType()),
            ("is_official_store", BooleanType()),
            ("status", StringType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["seller_id"],
    ),
    "customers": TableSpec(
        [
            ("customer_id", LongType()),
            ("platform_id", IntegerType()),
            ("platform_customer_id", StringType()),
            ("full_name", StringType()),
            ("email", StringType()),
            ("phone_number", StringType()),
            ("gender", StringType()),
            ("date_of_birth", DateType()),
            ("status", StringType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["customer_id"],
    ),
    "customer_addresses": TableSpec(
        [
            ("address_id", LongType()),
            ("customer_id", LongType()),
            ("recipient_name", StringType()),
            ("phone_number", StringType()),
            ("address_line", StringType()),
            ("ward", StringType()),
            ("district", StringType()),
            ("city", StringType()),
            ("province", StringType()),
            ("country", StringType()),
            ("postal_code", StringType()),
            ("is_default", BooleanType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["address_id"],
    ),
    "categories": TableSpec(
        [
            ("category_id", LongType()),
            ("platform_id", IntegerType()),
            ("platform_category_id", StringType()),
            ("parent_category_id", LongType()),
            ("category_name", StringType()),
            ("is_active", BooleanType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["category_id"],
    ),
    "brands": TableSpec(
        [
            ("brand_id", LongType()),
            ("platform_id", IntegerType()),
            ("platform_brand_id", StringType()),
            ("brand_name", StringType()),
            ("country", StringType()),
            ("is_official", BooleanType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["brand_id"],
    ),
    "products": TableSpec(
        [
            ("product_id", LongType()),
            ("platform_product_id", StringType()),
            ("seller_id", LongType()),
            ("category_id", LongType()),
            ("brand_id", LongType()),
            ("product_name", StringType()),
            ("description", StringType()),
            ("status", StringType()),
            ("is_authentic", BooleanType()),
            ("published_at", TimestampType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["product_id"],
    ),
    "product_variants": TableSpec(
        [
            ("variant_id", LongType()),
            ("product_id", LongType()),
            ("platform_variant_id", StringType()),
            ("sku", StringType()),
            ("variant_name", StringType()),
            ("original_price", DecimalType(18, 2)),
            ("sale_price", DecimalType(18, 2)),
            ("weight_gram", IntegerType()),
            ("status", StringType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["variant_id"],
    ),
    "product_inventory": TableSpec(
        [
            ("inventory_id", LongType()),
            ("variant_id", LongType()),
            ("warehouse_code", StringType()),
            ("quantity_on_hand", IntegerType()),
            ("quantity_reserved", IntegerType()),
            ("low_stock_threshold", IntegerType()),
            ("updated_at", TimestampType()),
        ],
        ["inventory_id"],
    ),
    "inventory_movements": TableSpec(
        [
            ("movement_id", LongType()),
            ("inventory_id", LongType()),
            ("movement_type", StringType()),
            ("quantity_delta", IntegerType()),
            ("reference_type", StringType()),
            ("reference_id", StringType()),
            ("reason", StringType()),
            ("occurred_at", TimestampType()),
        ],
        ["movement_id"],
    ),
    "vouchers": TableSpec(
        [
            ("voucher_id", LongType()),
            ("platform_id", IntegerType()),
            ("seller_id", LongType()),
            ("voucher_code", StringType()),
            ("voucher_name", StringType()),
            ("discount_type", StringType()),
            ("discount_value", DecimalType(18, 2)),
            ("max_discount_amount", DecimalType(18, 2)),
            ("min_order_amount", DecimalType(18, 2)),
            ("usage_limit", IntegerType()),
            ("starts_at", TimestampType()),
            ("ends_at", TimestampType()),
            ("status", StringType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["voucher_id"],
    ),
    "carts": TableSpec(
        [
            ("cart_id", LongType()),
            ("customer_id", LongType()),
            ("status", StringType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["cart_id"],
    ),
    "cart_items": TableSpec(
        [
            ("cart_item_id", LongType()),
            ("cart_id", LongType()),
            ("variant_id", LongType()),
            ("quantity", IntegerType()),
            ("unit_price", DecimalType(18, 2)),
            ("added_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["cart_item_id"],
    ),
    "orders": TableSpec(
        [
            ("order_id", LongType()),
            ("platform_order_id", StringType()),
            ("customer_id", LongType()),
            ("seller_id", LongType()),
            ("shipping_address_id", LongType()),
            ("voucher_id", LongType()),
            ("order_status", StringType()),
            ("subtotal_amount", DecimalType(18, 2)),
            ("shipping_fee", DecimalType(18, 2)),
            ("discount_amount", DecimalType(18, 2)),
            ("total_amount", DecimalType(18, 2)),
            ("ordered_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["order_id"],
    ),
    "order_items": TableSpec(
        [
            ("order_item_id", LongType()),
            ("order_id", LongType()),
            ("variant_id", LongType()),
            ("quantity", IntegerType()),
            ("unit_price", DecimalType(18, 2)),
            ("discount_amount", DecimalType(18, 2)),
        ],
        ["order_item_id"],
    ),
    "payments": TableSpec(
        [
            ("payment_id", LongType()),
            ("order_id", LongType()),
            ("payment_method", StringType()),
            ("provider", StringType()),
            ("amount", DecimalType(18, 2)),
            ("status", StringType()),
            ("paid_at", TimestampType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["payment_id"],
    ),
    "shipments": TableSpec(
        [
            ("shipment_id", LongType()),
            ("order_id", LongType()),
            ("carrier_name", StringType()),
            ("tracking_number", StringType()),
            ("shipping_method", StringType()),
            ("status", StringType()),
            ("shipped_at", TimestampType()),
            ("estimated_delivery_at", TimestampType()),
            ("delivered_at", TimestampType()),
            ("created_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["shipment_id"],
    ),
    "product_reviews": TableSpec(
        [
            ("review_id", LongType()),
            ("platform_review_id", StringType()),
            ("product_id", LongType()),
            ("order_item_id", LongType()),
            ("customer_id", LongType()),
            ("rating", IntegerType()),
            ("title", StringType()),
            ("content", StringType()),
            ("delivery_rating", IntegerType()),
            ("seller_rating", IntegerType()),
            ("helpful_count", IntegerType()),
            ("status", StringType()),
            ("reviewed_at", TimestampType()),
            ("updated_at", TimestampType()),
        ],
        ["review_id"],
    ),
    "events": TableSpec(
        [
            ("event_id", LongType()),
            ("platform_event_id", StringType()),
            ("customer_id", LongType()),
            ("product_id", LongType()),
            ("variant_id", LongType()),
            ("cart_item_id", LongType()),
            ("order_item_id", LongType()),
            ("event_type", StringType()),
            ("created_at", TimestampType()),
        ],
        ["event_id"],
    ),
}


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Stream Kafka CDC events already written to Bronze Delta, normalize them "
            "to Silver Delta tables, and register those tables in Hive Metastore."
        )
    )
    parser.add_argument(
        "--bronze-path",
        default=os.getenv("KAFKA_BRONZE_PATH", "s3a://bronze-lakehouse/kafka_cdc"),
        help="Bronze Delta path written by processing/streaming/kafka_to_bronze.py.",
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("SILVER_BASE_PATH", "s3a://silver-lakehouse"),
        help="Silver bucket/base path. Tables are written as <base>/<table_name>.",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=os.getenv(
            "BRONZE_TO_SILVER_CHECKPOINT_PATH",
            "s3a://silver-lakehouse/_checkpoints/bronze_to_silver",
        ),
        help="Structured Streaming checkpoint path for this job.",
    )
    parser.add_argument(
        "--hive-db",
        default=os.getenv("SILVER_HIVE_DATABASE", "silver"),
        help="Hive database name stored in the PostgreSQL Hive Metastore.",
    )
    parser.add_argument(
        "--processing-time",
        default=os.getenv("BRONZE_TO_SILVER_PROCESSING_TIME", "30 seconds"),
        help="Streaming trigger interval, for example: 30 seconds.",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process currently available Bronze data then stop. Useful for backfills/checks.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Read Bronze as a batch and stop. Easier to debug than Structured Streaming.",
    )
    parser.add_argument(
        "--skip-hive-sync",
        action="store_true",
        default=os.getenv("BRONZE_TO_SILVER_SKIP_HIVE_SYNC", "false").lower() == "true",
        help="Do not sync Delta table metadata to Hive Metastore after writing Silver.",
    )
    parser.add_argument(
        "--tables",
        default=os.getenv("BRONZE_TO_SILVER_TABLES"),
        help="Optional comma-separated source_table allow-list.",
    )
    return parser.parse_args()


def build_spark():
    spark = get_spark_session(
        app_name="BronzeToSilverStreaming",
        enable_hive_support=True,
        log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
    )
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
    return spark


def sql_identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return f"`{name}`"


def table_path(silver_base, table_name):
    return f"{silver_base.rstrip('/')}/{table_name}"


def bronze_payload_col():
    return F.when(F.col("debezium_op") == F.lit("d"), F.col("payload_before")).otherwise(
        F.col("payload_after")
    )


def json_scalar(payload, column_name):
    return F.get_json_object(payload, f"$.{column_name}")


def cast_json_value(raw_value, data_type):
    if isinstance(data_type, StringType):
        value = F.trim(raw_value.cast("string"))
        return F.when(F.length(value) > 0, value)

    if isinstance(data_type, BooleanType):
        return raw_value.cast("boolean")

    if isinstance(data_type, IntegerType):
        return raw_value.cast("int")

    if isinstance(data_type, LongType):
        return raw_value.cast("long")

    if isinstance(data_type, DecimalType):
        return raw_value.cast(data_type)

    if isinstance(data_type, DateType):
        text_value = raw_value.cast("string")
        days_from_epoch = text_value.cast("int")
        return F.when(
            text_value.rlike(r"^-?\d+$"),
            F.date_add(F.lit("1970-01-01").cast("date"), days_from_epoch),
        ).otherwise(F.to_date(text_value))

    if isinstance(data_type, TimestampType):
        text_value = raw_value.cast("string")
        numeric_value = text_value.cast("double")
        numeric_timestamp = (
            F.when(
                F.abs(numeric_value) >= F.lit(1000000000000000),
                F.to_timestamp(F.from_unixtime(numeric_value / F.lit(1000000))),
            )
            .when(
                F.abs(numeric_value) >= F.lit(1000000000000),
                F.to_timestamp(F.from_unixtime(numeric_value / F.lit(1000))),
            )
            .otherwise(F.to_timestamp(F.from_unixtime(numeric_value)))
        )
        normalized_text = F.regexp_replace(text_value, "T", " ")
        normalized_text = F.regexp_replace(normalized_text, "Z$", "")
        return F.when(text_value.rlike(r"^-?\d+(\.\d+)?$"), numeric_timestamp).otherwise(
            F.coalesce(F.to_timestamp(text_value), F.to_timestamp(normalized_text))
        )

    return raw_value


def default_value(data_type):
    if isinstance(data_type, BooleanType):
        return F.lit(None).cast("boolean")
    if isinstance(data_type, IntegerType):
        return F.lit(None).cast("int")
    if isinstance(data_type, LongType):
        return F.lit(None).cast("long")
    if isinstance(data_type, DecimalType):
        return F.lit(None).cast(data_type)
    if isinstance(data_type, DateType):
        return F.lit(None).cast("date")
    if isinstance(data_type, TimestampType):
        return F.lit(None).cast("timestamp")
    return F.lit(None).cast("string")


def normalize_table_events(batch_df, table_name, spec):
    payload = bronze_payload_col()
    selected_columns = []

    for column_name, data_type in spec.columns:
        raw_value = json_scalar(payload, column_name)
        selected_columns.append(
            F.coalesce(cast_json_value(raw_value, data_type), default_value(data_type)).alias(column_name)
        )

    normalized = batch_df.select(
        *selected_columns,
        F.col("event_date").cast("date").alias("event_date"),
        F.coalesce(F.col("debezium_op"), F.lit("r")).alias("_change_op"),
        F.col("kafka_timestamp").alias("_kafka_timestamp"),
        F.col("offset").cast("long").alias("_kafka_offset"),
    )

    key_is_present = None
    for key in spec.primary_keys:
        key_condition = F.col(key).isNotNull()
        key_is_present = key_condition if key_is_present is None else key_is_present & key_condition

    normalized = normalized.where(key_is_present)

    order_cols = [
        F.col("_kafka_timestamp").desc_nulls_last(),
        F.col("_kafka_offset").desc_nulls_last(),
    ]
    window = Window.partitionBy(*[F.col(key) for key in spec.primary_keys]).orderBy(*order_cols)

    return normalized.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def output_columns(spec):
    return [column_name for column_name, _ in spec.columns] + ["event_date"]


def non_delete_rows(df, spec):
    return df.where(F.col("_change_op") != F.lit("d")).select(*output_columns(spec))


def has_rows(df):
    return len(df.take(1)) > 0


def ensure_hive_table(spark, hive_db, table_name, target_path):
    sync_hive_delta_table(spark, hive_db, table_name, target_path)


def initialize_delta_table(df, spec, target_path):
    initial_rows = non_delete_rows(df, spec)
    if not has_rows(initial_rows):
        return False

    (
        initial_rows.repartition("event_date")
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .save(target_path)
    )
    return True


def merge_delta_table(df, spec, target_path):
    delta_table = DeltaTable.forPath(df.sparkSession, target_path)
    merge_condition = " AND ".join([f"target.`{key}` = source.`{key}`" for key in spec.primary_keys])

    values = {column: f"source.`{column}`" for column in output_columns(spec)}

    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedDelete(condition="source._change_op = 'd'")
        .whenMatchedUpdate(condition="source._change_op <> 'd'", set=values)
        .whenNotMatchedInsert(condition="source._change_op <> 'd'", values=values)
        .execute()
    )


def process_table(batch_df, table_name, spec, args):
    table_df = batch_df.where(F.col("source_table") == table_name)
    normalized_df = normalize_table_events(table_df, table_name, spec).persist()
    target_path = table_path(args.silver_base, table_name)

    try:
        raw_count = table_df.count()
        valid_count = normalized_df.count()
        if valid_count == 0:
            print(f"  - {table_name}: raw={raw_count}, hợp lệ=0, bỏ qua vì thiếu khóa chính.")
            return

        op_counts = {
            row["_change_op"]: row["count"]
            for row in normalized_df.groupBy("_change_op").count().collect()
        }
        print(
            f"  - {table_name}: raw={raw_count}, hợp lệ={valid_count}, "
            f"ops={op_counts}, path={target_path}"
        )

        if DeltaTable.isDeltaTable(batch_df.sparkSession, target_path):
            merge_delta_table(normalized_df, spec, target_path)
            print(f"    └─ MERGE xong {table_name}")
        else:
            created = initialize_delta_table(normalized_df, spec, target_path)
            if not created:
                print(f"  - {table_name}: chỉ có delete event, bỏ qua vì Delta table chưa tồn tại.")
                return
            print(f"    └─ Khởi tạo Delta table xong {table_name}")

        if args.skip_hive_sync:
            print(f"    └─ Bỏ qua Hive sync theo --skip-hive-sync")
        else:
            ensure_hive_table(batch_df.sparkSession, args.hive_db, table_name, target_path)
            print(f"    └─ Đồng bộ Hive Metastore xong {args.hive_db}.{table_name}")
    finally:
        normalized_df.unpersist()


def selected_tables(args):
    if not args.tables:
        return set(TABLE_SPECS)

    tables = {item.strip() for item in args.tables.split(",") if item.strip()}
    unknown_tables = sorted(tables - set(TABLE_SPECS))
    if unknown_tables:
        raise ValueError(f"Các bảng chưa được hỗ trợ theo init_schema.sql: {', '.join(unknown_tables)}")
    return tables


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


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Bronze -> Silver streaming failed: {exc}")
        sys.exit(1)
