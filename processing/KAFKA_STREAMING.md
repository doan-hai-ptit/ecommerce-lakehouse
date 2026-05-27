# Kafka Streaming Pipeline

## Design

The streaming path is:

```text
Postgres source -> Debezium -> Kafka -> Bronze Delta -> Silver Delta -> Hive Metastore/PostgreSQL
```

There are two Spark jobs:

- `streaming/kafka_to_bronze.py`: reads Kafka CDC topics and writes raw events to Bronze Delta.
- `jobs/bronze_to_silver.py`: reads Bronze Delta, normalizes each CDC table to Silver Delta, and optionally syncs Hive metadata.

Bronze path:

```text
s3a://bronze-lakehouse/kafka_cdc
```

Silver paths:

```text
s3a://silver-lakehouse/<table_name>
```

Default checkpoints:

```text
s3a://bronze-lakehouse/_checkpoints/kafka_to_bronze/<topic-or-pattern>
s3a://silver-lakehouse/_checkpoints/bronze_to_silver
```

The Bronze Delta table keeps Kafka metadata plus the raw value:

- `topic`, `partition`, `offset`
- `kafka_timestamp`, `kafka_timestamp_type`
- `message_key`, `message_value`, `headers_json`
- `debezium_op`, `source_db`, `source_schema`, `source_table`
- `payload_before`, `payload_after`
- `event_date`, `ingested_at`

Silver tables are partitioned by `event_date`. The table schema follows `database/init_schema.sql`.

## Build Spark image

Rebuild the Spark processor image after changing the Dockerfile:

```bash
docker compose build spark-processor
docker compose up -d kafka minio postgres postgres-data-source debezium spark-processor
```

## Run Full Streaming Pipeline

Start Kafka to Bronze in terminal 1:

```bash
docker exec -it spark_processor python /app/streaming/kafka_to_bronze.py
```

Start Bronze to Silver in terminal 2:

```bash
docker exec -it spark_processor python /app/jobs/bronze_to_silver.py
```

This processes all supported CDC tables found in Bronze and syncs Hive metadata by default.

## First Run And Daily Run

On the first run, or after changing schema/table locations, run with Hive sync enabled:

```bash
docker exec -it spark_processor python /app/jobs/bronze_to_silver.py --available-now
```

For long-running daily streaming after Hive tables are already registered, skip Hive sync to reduce work per micro-batch:

```bash
docker exec -it spark_processor python /app/jobs/bronze_to_silver.py --skip-hive-sync
```

New `event_date` partitions do not require Hive sync every day. Delta tracks new partitions in `_delta_log`, and Spark reads them through the Delta table metadata.

## Debug Commands

Read Bronze as a batch, process everything currently available, then stop:

```bash
docker exec -it spark_processor python /app/jobs/bronze_to_silver.py --once
```

Process only selected tables:

```bash
docker exec -it spark_processor python /app/jobs/bronze_to_silver.py --once --tables products,orders,order_items
```

Process selected tables and skip Hive sync:

```bash
docker exec -it spark_processor python /app/jobs/bronze_to_silver.py --once --tables platforms --skip-hive-sync
```

Process currently available Bronze data as a streaming query and stop:

```bash
docker exec -it spark_processor python /app/jobs/bronze_to_silver.py --available-now
```

## Kafka To Bronze Options

```bash
--bootstrap-servers kafka:9092
--starting-offsets earliest
--output-path s3a://bronze-lakehouse/kafka_cdc
--checkpoint-path s3a://bronze-lakehouse/_checkpoints/kafka_to_bronze/products
--max-offsets-per-trigger 10000
```

Example for one topic:

```bash
docker exec -it spark_processor python /app/streaming/kafka_to_bronze.py \
  --topics cdc.ecommerce.public.products
```

Example for all ecommerce CDC topics:

```bash
docker exec -it spark_processor python /app/streaming/kafka_to_bronze.py \
  --topic-pattern 'cdc.ecommerce.public.*'
```

## Bronze To Silver Options

```bash
--bronze-path s3a://bronze-lakehouse/kafka_cdc
--silver-base s3a://silver-lakehouse
--checkpoint-path s3a://silver-lakehouse/_checkpoints/bronze_to_silver
--hive-db silver
--processing-time "30 seconds"
--available-now
--once
--skip-hive-sync
--tables products,orders
```

You can also disable Hive sync with an environment variable:

```bash
BRONZE_TO_SILVER_SKIP_HIVE_SYNC=true
```

## Check Bronze Tables

```bash
docker exec -i spark_processor python - <<'PY'
import sys
sys.path.append('/app')
from core.spark_session import get_spark_session

spark = get_spark_session("CheckBronzeTables")
spark.read.format("delta").load("s3a://bronze-lakehouse/kafka_cdc") \
    .select("source_table").distinct().orderBy("source_table") \
    .show(50, truncate=False)
spark.stop()
PY
```

## Check Silver And Hive Metadata

```bash
docker exec -i spark_processor python - <<'PY'
import sys
sys.path.append('/app')
from core.spark_session import get_spark_session

spark = get_spark_session("CheckSilverHive", enable_hive_support=True)

spark.sql("SHOW DATABASES").show(truncate=False)
spark.sql("SHOW TABLES IN silver").show(100, truncate=False)
spark.sql("DESCRIBE DETAIL silver.products").select(
    "location", "format", "partitionColumns"
).show(truncate=False)

spark.sql("""
    SELECT event_date, COUNT(*) AS rows
    FROM silver.products
    GROUP BY event_date
    ORDER BY event_date
""").show(100, truncate=False)

spark.stop()
PY
```

## Check PostgreSQL Hive Metastore Directly

```bash
docker exec -it postgres_metastore psql -U postgres -d postgres_metastore
```

Then run:

```sql
SELECT d."NAME" AS db_name, t."TBL_NAME", s."LOCATION"
FROM "TBLS" t
JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
WHERE d."NAME" = 'silver'
ORDER BY t."TBL_NAME";

SELECT t."TBL_NAME", p."PARAM_KEY", p."PARAM_VALUE"
FROM "TABLE_PARAMS" p
JOIN "TBLS" t ON p."TBL_ID" = t."TBL_ID"
WHERE t."TBL_NAME" = 'products'
ORDER BY p."PARAM_KEY";
```

Expected table params include:

```text
spark.sql.sources.provider = delta
spark.sql.sources.schema.numPartCols = 1
spark.sql.sources.schema.partCol.0 = event_date
```

## Notes

- `database/init_schema.sql` defines 19 tables.
- Silver creates a table only after Bronze has CDC events for that `source_table`.
- If `vouchers` has no source data/events yet, Bronze, Silver, and Hive may show 18 tables until voucher data appears.
- Do not delete checkpoint paths unless you intentionally want to reprocess from the beginning.
- Reprocessing is idempotent for Silver because the job uses Delta `MERGE` by table primary key.
