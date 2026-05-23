import argparse
import os
import re
import sys

# Ensure parent processing/ directory is in sys.path so we can import core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from pyspark.sql import functions as F
from core.spark_session import get_spark_session

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read Kafka topics with Spark Structured Streaming and write raw events to Bronze Delta."
    )
    topic_group = parser.add_mutually_exclusive_group(required=True)
    topic_group.add_argument(
        "--topics",
        help="Comma-separated Kafka topics, for example: cdc.ecommerce.public.products",
    )
    topic_group.add_argument(
        "--topic-pattern",
        help="Kafka topic regex pattern, for example: cdc.ecommerce.public.*",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap servers. Default: env KAFKA_BOOTSTRAP_SERVERS or kafka:9092.",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
        help="Kafka starting offsets when no checkpoint exists.",
    )
    parser.add_argument(
        "--output-path",
        default=os.getenv("KAFKA_BRONZE_PATH", "s3a://bronze-lakehouse/kafka_cdc"),
        help="Bronze Delta output path.",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=os.getenv("KAFKA_CHECKPOINT_PATH"),
        help="Structured Streaming checkpoint path. Default is generated under s3a://bronze-lakehouse/_checkpoints.",
    )
    parser.add_argument(
        "--processing-time",
        default=os.getenv("KAFKA_PROCESSING_TIME", "30 seconds"),
        help="Streaming trigger interval, for example: 30 seconds.",
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "0")),
        help="Optional Kafka read throttle. 0 means unlimited.",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process currently available Kafka data then stop. Useful for local checks/backfills.",
    )
    return parser.parse_args()


def build_spark():
    return get_spark_session(
        app_name="KafkaToBronze",
        log_level=os.getenv("SPARK_LOG_LEVEL", "WARN")
    )


def make_checkpoint_path(args):
    if args.checkpoint_path:
        return args.checkpoint_path

    source_name = args.topics or args.topic_pattern or "kafka"
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", source_name).strip("_")
    return f"s3a://bronze-lakehouse/_checkpoints/kafka_to_bronze/{safe_name}"


def read_kafka_stream(spark, args):
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .option("includeHeaders", "true")
    )

    if args.max_offsets_per_trigger > 0:
        reader = reader.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)

    if args.topics:
        reader = reader.option("subscribe", args.topics)
    else:
        reader = reader.option("subscribePattern", args.topic_pattern)

    return reader.load()


def normalize_kafka_events(kafka_df):
    value_text = F.col("value").cast("string")
    key_text = F.col("key").cast("string")

    return (
        kafka_df.select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("timestampType").alias("kafka_timestamp_type"),
            key_text.alias("message_key"),
            value_text.alias("message_value"),
            F.expr(
                "to_json(transform(headers, x -> named_struct('key', x.key, 'value', cast(x.value as string))))"
            ).alias("headers_json"),
        )
        .withColumn("event_date", F.to_date("kafka_timestamp"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("debezium_op", F.get_json_object("message_value", "$.payload.op"))
        .withColumn("source_db", F.get_json_object("message_value", "$.payload.source.db"))
        .withColumn("source_schema", F.get_json_object("message_value", "$.payload.source.schema"))
        .withColumn("source_table", F.get_json_object("message_value", "$.payload.source.table"))
        .withColumn("payload_before", F.get_json_object("message_value", "$.payload.before"))
        .withColumn("payload_after", F.get_json_object("message_value", "$.payload.after"))
    )


def start_query(events_df, args):
    checkpoint_path = make_checkpoint_path(args)
    writer = (
        events_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("event_date", "topic")
        .queryName("KafkaToBronze")
    )

    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.processing_time)

    print(f"Kafka bootstrap servers: {args.bootstrap_servers}")
    print(f"Kafka source: {args.topics or args.topic_pattern}")
    print(f"Bronze Delta path: {args.output_path}")
    print(f"Checkpoint path: {checkpoint_path}")

    return writer.start(args.output_path)


def main():
    args = parse_args()
    spark = build_spark()

    try:
        kafka_df = read_kafka_stream(spark, args)
        events_df = normalize_kafka_events(kafka_df)
        query = start_query(events_df, args)
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
