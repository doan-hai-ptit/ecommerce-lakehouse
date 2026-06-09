import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from confluent_kafka import Consumer, KafkaError, TopicPartition
from deltalake import write_deltalake

# Load environment variables
load_dotenv()

def parse_args():
    parser = argparse.ArgumentParser(
        description="Read Kafka topics with confluent-kafka and write raw events to Bronze Delta using Pandas & deltalake."
    )
    parser.add_argument(
        "--topics",
        default=os.getenv("KAFKA_TOPICS"),
        help="Comma-separated Kafka topics, for example: cdc.ecommerce.public.products",
    )
    parser.add_argument(
        "--topic-pattern",
        default=os.getenv("KAFKA_TOPIC_PATTERN", "^cdc.ecommerce.public.*"),
        help="Kafka topic regex pattern, for example: ^cdc.ecommerce.public.*",
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
        help="Kafka starting offsets when no committed offsets exist.",
    )
    parser.add_argument(
        "--output-path",
        default=os.getenv("KAFKA_BRONZE_PATH", "s3a://bronze-lakehouse/kafka_cdc"),
        help="Bronze Delta output path.",
    )
    parser.add_argument(
        "--batch-interval",
        type=float,
        default=float(os.getenv("KAFKA_BATCH_INTERVAL", "5.0")),
        help="Batch trigger interval in seconds. Default: 5.0.",
    )
    parser.add_argument(
        "--max-batch-size",
        type=int,
        default=int(os.getenv("KAFKA_MAX_BATCH_SIZE", "1000")),
        help="Maximum records per batch. Default: 1000.",
    )
    return parser.parse_args()

def get_storage_options():
    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
    
    # Auto-resolve minio host to localhost if running outside docker environment
    import socket
    from urllib.parse import urlparse
    parsed = urlparse(endpoint_url)
    if parsed.hostname == "minio":
        try:
            socket.gethostbyname("minio")
        except socket.gaierror:
            new_netloc = parsed.netloc.replace("minio", "localhost")
            endpoint_url = parsed._replace(netloc=new_netloc).geturl()
            
    return {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "password123"),
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

def extract_message_data(msg):
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()
    
    # Timestamp parsing
    ts_type, ts_val = msg.timestamp()
    if ts_val > 0:
        # Kafka timestamp is in milliseconds
        kafka_timestamp = pd.to_datetime(ts_val, unit="ms")
    else:
        kafka_timestamp = pd.Timestamp.now()
        
    event_date = kafka_timestamp.strftime("%Y-%m-%d")
    
    # Decode key and value
    message_key = msg.key().decode("utf-8") if msg.key() else None
    message_value = msg.value().decode("utf-8") if msg.value() else None
    
    # Headers JSON
    headers = msg.headers()
    headers_json = None
    if headers:
        headers_dict = [{k: v.decode("utf-8") if v else None} for k, v in headers]
        headers_json = json.dumps(headers_dict)
        
    # Extract Debezium payload metadata
    debezium_op = None
    source_db = None
    source_schema = None
    source_table = None
    payload_before = None
    payload_after = None
    
    if message_value:
        try:
            val_dict = json.loads(message_value)
            payload = val_dict.get("payload")
            if isinstance(payload, dict):
                debezium_op = payload.get("op")
                source = payload.get("source")
                if isinstance(source, dict):
                    source_db = source.get("db")
                    source_schema = source.get("schema")
                    source_table = source.get("table")
                
                # Serialized payload blocks
                before = payload.get("before")
                after = payload.get("after")
                payload_before = json.dumps(before) if before is not None else None
                payload_after = json.dumps(after) if after is not None else None
        except Exception as e:
            # Silently catch parsing errors for corrupt messages
            pass

    return {
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "kafka_timestamp": kafka_timestamp,
        "kafka_timestamp_type": ts_type,
        "message_key": message_key,
        "message_value": message_value,
        "headers_json": headers_json,
        "event_date": event_date,
        "ingested_at": pd.Timestamp.now(),
        "debezium_op": debezium_op,
        "source_db": source_db,
        "source_schema": source_schema,
        "source_table": source_table,
        "payload_before": payload_before,
        "payload_after": payload_after,
    }

def write_batch(records, output_path, storage_options):
    if not records:
        return
        
    df = pd.DataFrame(records)
    
    # delta-rs requires S3 URLs to start with s3:// instead of s3a://
    s3_path = output_path.replace("s3a://", "s3://")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Writing {len(df)} records to Bronze Delta: {s3_path}")
    
    write_deltalake(
        s3_path,
        df,
        mode="append",
        partition_by=["event_date", "topic"],
        storage_options=storage_options,
    )

def main():
    args = parse_args()
    
    # Kafka Consumer Configuration
    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "pandas-bronze-ingestion",
        "auto.offset.reset": args.starting_offsets,
        "enable.auto.commit": False, # Manual commit for reliability
    }
    
    consumer = Consumer(conf)
    
    # Subscribe using regex pattern or list
    if args.topics:
        topics_list = [t.strip() for t in args.topics.split(",") if t.strip()]
        consumer.subscribe(topics_list)
        print(f"Subscribed to topics list: {topics_list}")
    else:
        # confluent-kafka uses regex if prefixed with ^
        pattern = args.topic_pattern if args.topic_pattern.startswith("^") else f"^{args.topic_pattern}"
        consumer.subscribe([pattern])
        print(f"Subscribed to topic pattern: {pattern}")
        
    storage_options = get_storage_options()
    
    print(f"Bronze Ingestion started (Kafka: {args.bootstrap_servers} -> MinIO: {args.output_path})")
    print(f"Batch interval: {args.batch_interval}s, Max batch size: {args.max_batch_size}")
    
    records = []
    # Track partition offsets to commit after write
    offsets_to_commit = {}
    last_write_time = time.time()
    
    try:
        while True:
            # Poll with timeout
            msg = consumer.poll(timeout=0.5)
            
            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Kafka error: {msg.error()}")
                        break
                
                # Extract and append record
                record = extract_message_data(msg)
                records.append(record)
                
                # Track maximum offset for committing
                tp_key = (msg.topic(), msg.partition())
                offsets_to_commit[tp_key] = msg.offset()
                
            # Trigger write if time interval elapsed or max batch size reached
            time_elapsed = time.time() - last_write_time
            if (len(records) >= args.max_batch_size) or (time_elapsed >= args.batch_interval and len(records) > 0):
                try:
                    write_batch(records, args.output_path, storage_options)
                    
                    # Commit Kafka offsets after successful write
                    commit_partitions = [
                        TopicPartition(topic, partition, offset + 1)
                        for (topic, partition), offset in offsets_to_commit.items()
                    ]
                    consumer.commit(offsets=commit_partitions, asynchronous=False)
                    
                    # Reset batch state
                    records = []
                    offsets_to_commit = {}
                    last_write_time = time.time()
                except Exception as write_err:
                    print(f"Error writing to Delta table or committing offsets: {write_err}")
                    # Keep records to retry on the next loop iteration, do not commit offsets
                    time.sleep(2)
                    
    except KeyboardInterrupt:
        print("\nStopping ingestion...")
    finally:
        consumer.close()
        print("Consumer stopped.")

if __name__ == "__main__":
    main()
