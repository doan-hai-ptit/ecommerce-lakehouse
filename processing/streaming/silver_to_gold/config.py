import argparse
import os
from .builders import BUILDERS


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
