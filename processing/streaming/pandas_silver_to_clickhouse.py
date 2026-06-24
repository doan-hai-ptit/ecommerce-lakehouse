import os
import sys
import time
import argparse
import urllib.request
import urllib.parse
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

# Ensure parent processing/ directory and streaming/ directory are in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

# Import builders and configs from existing pandas_silver_to_gold script
from pandas_silver_to_gold import (
    get_storage_options,
    build_dim_platforms,
    build_dim_brands,
    build_dim_sellers,
    build_dim_customers,
    build_dim_products,
    build_dim_product_variants,
    build_dim_date,
    build_fct_orders,
    build_fct_order_items,
    build_fct_product_reviews,
    build_fct_shipments,
    PRIMARY_KEYS
)

# Registry of gold builders to execute
BUILDERS = {
    "dim_platforms": build_dim_platforms,
    "dim_brands": build_dim_brands,
    "dim_sellers": build_dim_sellers,
    "dim_customers": build_dim_customers,
    "dim_products": build_dim_products,
    "dim_product_variants": build_dim_product_variants,
    "fct_orders": build_fct_orders,
    "fct_order_items": build_fct_order_items,
    "fct_product_reviews": build_fct_product_reviews,
    "fct_shipments": build_fct_shipments,
}

# ClickHouse Configuration
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse_server")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "password123")
DEFAULT_CH_DB = os.getenv("CLICKHOUSE_GOLD_DATABASE", os.getenv("CLICKHOUSE_DATABASE", "gold_serving"))

def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream/Sync Gold layer tables directly from Silver Delta Lake to ClickHouse."
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("SILVER_BASE_PATH", "s3a://silver-lakehouse"),
        help="Silver bucket/base path.",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Optional comma-separated list of target tables to sync.",
    )
    parser.add_argument(
        "--database",
        default=DEFAULT_CH_DB,
        help="Target ClickHouse database. Default: gold_serving.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=15.0,
        help="Sync interval in seconds. Default: 15.0s.",
    )
    parser.add_argument(
        "--mode",
        choices=["append", "replace"],
        default="append",
        help="append keeps ReplacingMergeTree versions; replace truncates each target before inserting the current snapshot.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit.",
    )
    return parser.parse_args()

def execute_clickhouse_query(query, database=None):
    url = f"http://{CH_HOST}:{CH_PORT}/"
    params = {
        "user": CH_USER,
        "password": CH_PASSWORD,
    }
    if database:
        params["database"] = database
    full_url = f"{url}?{urllib.parse.urlencode(params)}"

    req = urllib.request.Request(
        full_url,
        data=query.encode('utf-8'),
        method='POST'
    )

    try:
        with urllib.request.urlopen(req) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        if hasattr(e, 'read'):
            error_details = e.read().decode('utf-8')
            raise Exception(f"ClickHouse Error: {error_details}")
        raise e

def pandas_type_to_clickhouse(column_name, series):
    if column_name in {"date_actual", "date_of_birth"}:
        return "Date32"
    if pd.api.types.is_integer_dtype(series):
        return "Int64"
    if pd.api.types.is_float_dtype(series):
        return "Float64"
    if pd.api.types.is_bool_dtype(series):
        return "Bool"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "DateTime64(6, 'UTC')"
    return "String"

def ensure_clickhouse_table(df, table_name, database):
    pk_cols = PRIMARY_KEYS.get(table_name, [])
    columns_ddl = []
    for col_name in df.columns:
        ch_type = pandas_type_to_clickhouse(col_name, df[col_name])
        if col_name in pk_cols or col_name == "updated_at":
            columns_ddl.append(f"    `{col_name}` {ch_type}")
        else:
            columns_ddl.append(f"    `{col_name}` Nullable({ch_type})")

    pk_str = ", ".join([f"`{pk}`" for pk in pk_cols])
    engine = "ReplacingMergeTree(`updated_at`)" if "updated_at" in df.columns else "ReplacingMergeTree()"
    ddl = f"""
CREATE TABLE IF NOT EXISTS {database}.{table_name}
(
{",\n".join(columns_ddl)}
)
ENGINE = {engine}
ORDER BY ({pk_str})
"""
    execute_clickhouse_query(ddl, database=database)

def insert_dataframe_to_clickhouse(df, table_name, database, mode="append"):
    """
    Sends Pandas DataFrame rows to ClickHouse using HTTP JSONEachRow format.
    """
    url = f"http://{CH_HOST}:{CH_PORT}/"
    
    # Copy DataFrame to avoid modifying the original data in-place
    df_copy = df.copy()
    
    # Format Date/Date32 fields to YYYY-MM-DD strings so ClickHouse can parse them successfully
    for date_col in ['date_actual', 'date_of_birth']:
        if date_col in df_copy.columns:
            try:
                df_copy[date_col] = pd.to_datetime(df_copy[date_col]).dt.strftime('%Y-%m-%d')
            except Exception:
                pass
                
    # ClickHouse expects a POST body where each line is a JSON object (JSONEachRow)
    # Convert datetimes to isoformat strings for easy parsing by ClickHouse
    json_lines = df_copy.to_json(orient='records', lines=True, date_format='iso')
    
    if not json_lines.strip():
        return

    ensure_clickhouse_table(df_copy, table_name, database)

    if mode == "replace":
        execute_clickhouse_query(f"TRUNCATE TABLE {database}.{table_name}", database=database)
        
    params = {
        "user": CH_USER,
        "password": CH_PASSWORD,
        "database": database,
        "query": f"INSERT INTO {table_name} FORMAT JSONEachRow",
        "date_time_input_format": "best_effort"
    }
    
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    
    req = urllib.request.Request(
        full_url,
        data=json_lines.encode('utf-8'),
        method='POST',
        headers={"Content-Type": "application/x-ndjson"}
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            response.read()
    except Exception as e:
        if hasattr(e, 'read'):
            error_details = e.read().decode('utf-8')
            raise Exception(f"ClickHouse Error during insert into {table_name}: {error_details}")
        raise e

def sync_table(table_name, builder_func, silver_base, storage_options, database, mode):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Syncing {table_name}...")
    
    # 1. Build Gold dataframe from Silver
    df = builder_func(silver_base, storage_options)
    
    if df is None or df.empty:
        print(f"  No data found or empty dataframe for {table_name}.")
        return
        
    # 2. Pre-process dataframe to align with ReplacingMergeTree requirements:
    # Order By and Version columns cannot contain Null values.
    pk_cols = PRIMARY_KEYS.get(table_name, [])
    
    # Drop rows containing null in primary keys
    initial_len = len(df)
    df = df.dropna(subset=pk_cols)
    
    # Drop rows containing null in updated_at version column
    if 'updated_at' in df.columns:
        df = df.dropna(subset=['updated_at'])
        
    if len(df) < initial_len:
        print(f"  Dropped {initial_len - len(df)} rows due to null PKs or updated_at.")
        
    if df.empty:
        print(f"  Dataframe is empty after cleanup.")
        return
        
    # 3. Insert into ClickHouse
    try:
        insert_dataframe_to_clickhouse(df, table_name, database, mode=mode)
        print(f"  ✔ Successfully synchronized {len(df)} rows to ClickHouse!")
    except Exception as e:
        print(f"  ❌ Error syncing to ClickHouse: {e}")

def main():
    args = parse_args()
    storage_options = get_storage_options()
    
    allowed_tables = set(BUILDERS.keys())
    if args.tables:
        allowed_tables = {t.strip() for t in args.tables.split(",") if t.strip()}
        
    print(f"Silver to ClickHouse Sync Engine Started (Interval: {args.interval}s)")
    print(f"Silver Base: {args.silver_base}")
    print(f"ClickHouse Database: {args.database}")
    print(f"Mode: {args.mode}")
    print(f"Target Tables: {', '.join(allowed_tables)}")
    execute_clickhouse_query(f"CREATE DATABASE IF NOT EXISTS {args.database}")
    
    try:
        while True:
            # 1. Update/Sync dim_date first
            if "dim_date" in allowed_tables or args.tables is None:
                try:
                    dim_date_df = build_dim_date()
                    # dim_date doesn't have updated_at, just standard insert
                    insert_dataframe_to_clickhouse(dim_date_df, "dim_date", args.database, mode=args.mode)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✔ Synchronized dim_date")
                except Exception as e:
                    print(f"Error syncing dim_date: {e}")
                    
            # 2. Update/Sync other Gold tables
            for table_name in sorted(allowed_tables):
                if table_name == "dim_date":
                    continue
                builder_func = BUILDERS.get(table_name)
                if builder_func:
                    try:
                        sync_table(table_name, builder_func, args.silver_base, storage_options, args.database, args.mode)
                    except Exception as e:
                        print(f"Error during execution of {table_name}: {e}")
                        
            if args.once:
                print("Run once completed. Exiting.")
                break
            print(f"Sleeping for {args.interval}s...\n")
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\nStopping Silver to ClickHouse Sync Engine...")

if __name__ == "__main__":
    main()
