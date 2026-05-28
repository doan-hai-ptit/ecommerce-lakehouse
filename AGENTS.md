# AGENTS.md

## Project Overview

This repo is an ecommerce lakehouse project. It crawls ecommerce product and
review data, stores raw JSON in MinIO Bronze, then processes it with
Spark/Delta Lake into Silver.

Main components:

- `ingestion/`: batch crawlers for Tiki, Shopee, Sendo, and ChoTot.
- `ingestion/batch/providers/`: source-specific API/Selenium clients.
- `processing/`: Spark jobs for Bronze to Silver (and Silver to Gold) transformation.
  - `processing/streaming/bronze_to_silver/`: Refactored modular subpackage for Bronze-to-Silver CDC pipeline (schemas, configs, utils, transformer, writer, orchestrator).
  - `processing/streaming/silver_to_gold/`: Refactored modular subpackage for Silver-to-Gold dimensional pipeline (builders, configs, orchestrator).
- `airflow/`: Airflow DAGs for orchestration.
- `storage/`: MinIO utilities and data-reading scripts.
- `monitoring/`: Prometheus configuration.
- `simulator/`: optional simulation scripts.
- `docker-compose.yml`: main local infrastructure: MinIO, Postgres metastore,
  Spark processor, and Browserless.

## Development Rules

- Keep changes scoped to the requested feature or bug.
- Do not rewrite the whole pipeline unless explicitly asked.
- Preserve the existing folder structure.
- Prefer small, readable Python functions over large rewrites.
- Use environment variables for credentials and service URLs.
- Do not hardcode secrets beyond existing local-dev defaults.
- Do not commit generated raw data under `ingestion/batch/raw_data/` unless
  specifically requested.
- Be careful with Docker volumes and MinIO/Postgres state.

## Python Style

- Use Python 3 style.
- Prefer explicit names for pipeline stages, providers, and storage paths.
- Keep provider-specific logic inside `ingestion/batch/providers/`.
- Keep transformation logic inside `processing/`.
- Avoid mixing crawling, storage, and transformation concerns in one file unless
  the current file already does so.

## Data Layout

Bronze data is stored in MinIO using Hive-style partitions:

```text
provider=<source>/date=<yyyy-mm-dd>/category=<category>/<file>.json
```

Examples:

```text
provider=tiki/date=2026-05-15/category=products/batch_pg1_*.json
provider=tiki/date=2026-05-15/category=reviews/reviews_sp_*.json
```

Silver data is written as Delta Lake, for example:

```text
s3a://silver-lakehouse/ecom_products/platform=tiki
```

When changing paths, keep them compatible with Spark and Hive-style
partitioning.

## Local Services

The main services are defined in `docker-compose.yml`:

- MinIO: `localhost:9000`, console `localhost:9001`
- Postgres metastore: `localhost:5432`
- Spark processor: `localhost:4040`
- Browserless Chrome: `localhost:3000`

Default local credentials currently used in compose:

```text
MinIO user: admin
MinIO password: password123
Postgres user: postgres
Postgres password: postgres
Postgres DB: postgres_metastore
```

## Common Commands

Start main services:

```bash
docker compose up -d
```

Run Tiki ingestion locally:

```bash
python ingestion/batch/main.py --category 1846 --limit_pages 1
```

Run Bronze to Silver processing inside the Spark environment:

```bash
python processing/jobs/bronze_to_silver.py tiki
```

Test lakehouse connectivity:

```bash
python processing/test_lakehouse.py
```

Check MinIO connection:

```bash
python storage/check_minio_connection.py
```

## Important Notes

- Spark jobs `bronze_to_silver` and `silver_to_gold` are modularized under subpackages in `processing/streaming/`. They are invoked using the same entry-point commands as before (e.g. `processing/jobs/bronze_to_silver.py`) due to backward-compatible wrappers.
- `processing/jobs/bronze_to_silver.py` currently supports `tiki`, `sendo`, and
  `shopee`.
- Tiki crawling uses Selenium Remote WebDriver through Browserless.
- The crawler writes directly to MinIO using `boto3.put_object`.
- Postgres is used to track crawler state in `crawler_state`.
- Spark jobs depend on MinIO credentials from `.env` files.
- Some Kafka/MySQL/Debezium services are commented out in compose and should
  not be enabled unless requested.

## Testing Guidance

Before finishing changes, run the smallest relevant check:

- For crawler/provider changes, run a one-page crawl if services are available.
- For Spark transformation changes, run `processing/jobs/bronze_to_silver.py
  <source>`.
- For infrastructure changes, run `docker compose config`.
- If services are unavailable, explain which command should be run and why it
  was not executed.

## Agent Behavior

When modifying this repo:

- Read the nearby code before editing.
- Do not delete user data, raw crawl outputs, Docker volumes, or `.env` files.
- Do not run destructive Docker commands such as volume removal unless
  explicitly asked.
- Keep Vietnamese comments/messages if the surrounding file already uses
  Vietnamese.
- Summarize changed files and verification steps at the end.
