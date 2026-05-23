# Kafka to Spark Streaming

## Design

The streaming path is:

```text
Debezium/Postgres or producer -> Kafka -> Spark Structured Streaming -> Bronze Delta on MinIO
```

The Spark job reads Kafka topics and writes raw events to:

```text
s3a://bronze-lakehouse/kafka_cdc
```

Checkpoint state is stored separately so Spark can resume offsets safely:

```text
s3a://bronze-lakehouse/_checkpoints/kafka_to_bronze/<topic-or-pattern>
```

The Bronze Delta table keeps Kafka metadata plus the raw value:

- `topic`, `partition`, `offset`
- `kafka_timestamp`, `kafka_timestamp_type`
- `message_key`, `message_value`, `headers_json`
- `debezium_op`, `source_db`, `source_schema`, `source_table`
- `payload_before`, `payload_after`
- `event_date`, `ingested_at`

## Build Spark image

Rebuild the Spark processor image after changing the Dockerfile:

```bash
docker compose build spark-processor
docker compose up -d kafka minio postgres spark-processor
```

## Run one topic

For a Debezium table topic:

```bash
docker exec -it spark_processor python /app/streaming/kafka_to_bronze.py \
  --topics cdc.ecommerce.public.products
```

Process available data and stop, useful for a local check:

```bash
docker exec -it spark_processor python /app/streaming/kafka_to_bronze.py \
  --topics cdc.ecommerce.public.products \
  --available-now
```

## Run all ecommerce CDC topics

```bash
docker exec -it spark_processor python /app/streaming/kafka_to_bronze.py \
  --topic-pattern 'cdc.ecommerce.public.*'
```

## Useful options

```bash
--bootstrap-servers kafka:9092
--starting-offsets earliest
--output-path s3a://bronze-lakehouse/kafka_cdc
--checkpoint-path s3a://bronze-lakehouse/_checkpoints/kafka_to_bronze/products
--max-offsets-per-trigger 10000
```
