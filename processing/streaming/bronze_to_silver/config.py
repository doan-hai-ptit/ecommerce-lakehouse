import argparse
import os
from .schemas import TABLE_SPECS


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


def selected_tables(args):
    if not args.tables:
        return set(TABLE_SPECS)

    tables = {item.strip() for item in args.tables.split(",") if item.strip()}
    unknown_tables = sorted(tables - set(TABLE_SPECS))
    if unknown_tables:
        raise ValueError(f"Các bảng chưa được hỗ trợ theo init_schema.sql: {', '.join(unknown_tables)}")
    return tables
