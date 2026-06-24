import argparse
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime

import pandas as pd
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from dotenv import load_dotenv

# Ensure processing/ is importable when this script is run from the project root.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse_server")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "password123")

DEFAULT_DATABASE = os.getenv("CLICKHOUSE_SILVER_REAL_DATABASE", "silver_real_serving")
DEFAULT_SILVER_BASE = os.getenv("REAL_SILVER_BASE_PATH", "s3a://silver-lakehouse/real_data")

PRIMARY_KEYS = {
    "products": ["platform_product_id"],
    "sellers": ["platform_seller_id"],
    "product_reviews": ["platform_review_id"],
    "customers": ["platform_customer_id"],
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sync real Tiki batch Silver Delta tables to ClickHouse."
    )
    parser.add_argument(
        "--silver-base",
        default=DEFAULT_SILVER_BASE,
        help="Real Silver Delta base path. Default: s3a://silver-lakehouse/real_data.",
    )
    parser.add_argument(
        "--database",
        default=DEFAULT_DATABASE,
        help="Target ClickHouse database. Default: silver_real_serving.",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Optional comma-separated table list. Defaults to all real Silver tables.",
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="replace truncates the ClickHouse table before loading the current Delta snapshot.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=60.0,
        help="Sync interval in seconds when running continuously. Default: 60.0.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one sync pass and exit.",
    )
    return parser.parse_args()


def resolve_endpoint(endpoint_url):
    import socket
    from urllib.parse import urlparse

    parsed = urlparse(endpoint_url)
    if parsed.hostname == "minio":
        try:
            socket.gethostbyname("minio")
        except socket.gaierror:
            new_netloc = parsed.netloc.replace("minio", "localhost")
            endpoint_url = parsed._replace(netloc=new_netloc).geturl()
    return endpoint_url


def get_storage_options():
    endpoint_url = resolve_endpoint(os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
    return {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "password123"),
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def clickhouse_request(query, database=None, body=None, content_type="text/plain"):
    params = {
        "user": CH_USER,
        "password": CH_PASSWORD,
    }
    if database:
        params["database"] = database
    if query:
        params["query"] = query

    url = f"http://{CH_HOST}:{CH_PORT}/?{urllib.parse.urlencode(params)}"
    data = body.encode("utf-8") if isinstance(body, str) else body
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": content_type},
    )

    try:
        with urllib.request.urlopen(req) as response:
            return response.read().decode("utf-8")
    except Exception as exc:
        if hasattr(exc, "read"):
            details = exc.read().decode("utf-8")
            raise RuntimeError(f"ClickHouse error: {details}") from exc
        raise


def map_type_to_clickhouse(field_type):
    type_str = str(field_type).lower()
    if "string" in type_str:
        return "String"
    if "int64" in type_str or "long" in type_str:
        return "Int64"
    if "int32" in type_str or "integer" in type_str:
        return "Int32"
    if "int16" in type_str or "short" in type_str:
        return "Int16"
    if "int8" in type_str or "byte" in type_str:
        return "Int8"
    if "float64" in type_str or "double" in type_str:
        return "Float64"
    if "float32" in type_str or "float" in type_str:
        return "Float32"
    if "boolean" in type_str or "bool" in type_str:
        return "Bool"
    if "timestamp" in type_str:
        return "DateTime64(6, 'UTC')"
    if "date" in type_str:
        return "Date32"
    if "decimal" in type_str:
        return "Decimal128(9)"
    return "String"


def ensure_clickhouse_table(database, table_name, delta_table):
    primary_keys = PRIMARY_KEYS[table_name]
    columns = []
    for field in delta_table.schema().fields:
        ch_type = map_type_to_clickhouse(field.type)
        if field.name in primary_keys:
            columns.append(f"    `{field.name}` {ch_type}")
        else:
            columns.append(f"    `{field.name}` Nullable({ch_type})")

    pk_expr = ", ".join(f"`{key}`" for key in primary_keys)
    ddl = f"""
CREATE TABLE IF NOT EXISTS {database}.{table_name}
(
{",\n".join(columns)}
)
ENGINE = ReplacingMergeTree()
ORDER BY ({pk_expr})
"""
    clickhouse_request(f"CREATE DATABASE IF NOT EXISTS {database}")
    clickhouse_request(ddl, database=database)


def normalize_dataframe(df):
    df = df.copy()
    df = df.replace({pd.NaT: None})

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            df[col] = df[col].where(df[col].notna(), None)
        elif pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].astype("boolean")

    return df.where(pd.notnull(df), None)


def insert_dataframe(database, table_name, df):
    if df.empty:
        return

    payload = normalize_dataframe(df).to_json(
        orient="records",
        lines=True,
        date_format="iso",
        force_ascii=False,
    )
    if not payload.strip():
        return

    query = f"INSERT INTO {table_name} FORMAT JSONEachRow"
    clickhouse_request(
        query,
        database=database,
        body=payload,
        content_type="application/x-ndjson",
    )


def read_delta_snapshot(silver_base, table_name, storage_options):
    path = f"{silver_base.rstrip('/')}/{table_name}".replace("s3a://", "s3://")
    dt = DeltaTable(path, storage_options=storage_options)
    return dt, dt.to_pyarrow_table().to_pandas()


def sync_table(database, silver_base, table_name, storage_options, mode):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Syncing {table_name}...")

    try:
        delta_table, df = read_delta_snapshot(silver_base, table_name, storage_options)
    except TableNotFoundError:
        print(f"  Delta table not found. Skipping {table_name}.")
        return

    ensure_clickhouse_table(database, table_name, delta_table)

    primary_keys = PRIMARY_KEYS[table_name]
    initial_len = len(df)
    df = df.dropna(subset=primary_keys)
    df = df.drop_duplicates(subset=primary_keys, keep="last")

    if len(df) < initial_len:
        print(f"  Dropped {initial_len - len(df)} duplicate/null-key rows.")

    if mode == "replace":
        clickhouse_request(f"TRUNCATE TABLE {database}.{table_name}", database=database)

    insert_dataframe(database, table_name, df)
    print(f"  Inserted {len(df)} rows into {database}.{table_name}.")


def main():
    args = parse_args()
    storage_options = get_storage_options()
    tables = list(PRIMARY_KEYS)
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    unknown_tables = sorted(set(tables) - set(PRIMARY_KEYS))
    if unknown_tables:
        raise ValueError(f"Unsupported real Silver table(s): {', '.join(unknown_tables)}")

    print("Real Silver to ClickHouse sync started")
    print(f"Silver base: {args.silver_base}")
    print(f"ClickHouse database: {args.database}")
    print(f"Mode: {args.mode}")
    print(f"Target tables: {', '.join(tables)}")

    try:
        while True:
            for table_name in tables:
                try:
                    sync_table(args.database, args.silver_base, table_name, storage_options, args.mode)
                except Exception as exc:
                    print(f"Error syncing {table_name}: {exc}")

            if args.once:
                break

            print(f"Sleeping for {args.interval}s...\n")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopping real Silver to ClickHouse sync.")


if __name__ == "__main__":
    main()
