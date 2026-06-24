import argparse
import os
import urllib.request
import urllib.parse
from dotenv import load_dotenv
from deltalake import DeltaTable

load_dotenv()

# ClickHouse connection details (inside docker network by default)
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse_server")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "password123")

# Map Delta/PyArrow types to ClickHouse types
def map_type_to_clickhouse(field_name, field_type):
    type_str = str(field_type).lower()
    
    # Check for specific patterns
    if "string" in type_str:
        return "String"
    elif "int64" in type_str or "long" in type_str:
        return "Int64"
    elif "int32" in type_str or "integer" in type_str:
        return "Int32"
    elif "int16" in type_str or "short" in type_str:
        return "Int16"
    elif "int8" in type_str or "byte" in type_str:
        return "Int8"
    elif "float64" in type_str or "double" in type_str:
        return "Float64"
    elif "float32" in type_str or "float" in type_str:
        return "Float32"
    elif "boolean" in type_str or "bool" in type_str:
        return "Bool"
    elif "timestamp" in type_str:
        return "DateTime64(3, 'UTC')"
    elif "date" in type_str:
        return "Date32"
    elif "decimal" in type_str:
        return "Decimal128(9)" # default decimal representation
    else:
        return "String" # fallback

# Primary keys mapping for Gold tables
GOLD_PRIMARY_KEYS = {
    "dim_platforms": ["platform_id"],
    "dim_brands": ["brand_id"],
    "dim_sellers": ["seller_id"],
    "dim_customers": ["customer_id"],
    "dim_products": ["product_id"],
    "dim_product_variants": ["variant_id"],
    "dim_date": ["date_key"],
    "fct_orders": ["order_id"],
    "fct_order_items": ["order_item_id"],
    "fct_product_reviews": ["review_id"],
    "fct_shipments": ["shipment_id"],
}

SILVER_REAL_PRIMARY_KEYS = {
    "products": ["platform_product_id"],
    "sellers": ["platform_seller_id"],
    "product_reviews": ["platform_review_id"],
    "customers": ["platform_customer_id"],
}

LAYER_CONFIGS = {
    "gold": {
        "database": os.getenv("CLICKHOUSE_GOLD_DATABASE", "gold_serving"),
        "base_path": os.getenv("GOLD_BASE_PATH", "s3a://gold-lakehouse"),
        "primary_keys": GOLD_PRIMARY_KEYS,
    },
    "silver-real": {
        "database": os.getenv("CLICKHOUSE_SILVER_REAL_DATABASE", "silver_real_serving"),
        "base_path": os.getenv("REAL_SILVER_BASE_PATH", "s3a://silver-lakehouse/real_data"),
        "primary_keys": SILVER_REAL_PRIMARY_KEYS,
    },
}

def parse_args():
    parser = argparse.ArgumentParser(
        description="Create ClickHouse tables from Delta Lake table schemas."
    )
    parser.add_argument(
        "--layer",
        choices=sorted(LAYER_CONFIGS),
        default="gold",
        help="Known lakehouse layer/table set to create. Default: gold.",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Target ClickHouse database. Defaults depend on --layer.",
    )
    parser.add_argument(
        "--base-path",
        default=None,
        help="Delta Lake base path. Defaults depend on --layer.",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Optional comma-separated table list.",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Drop and recreate target ClickHouse tables before applying generated DDL.",
    )
    return parser.parse_args()

# Resolve minio host for container network
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

def main():
    args = parse_args()
    layer_config = LAYER_CONFIGS[args.layer]
    ch_db = args.database or layer_config["database"]
    base_path = args.base_path or layer_config["base_path"]
    primary_keys_by_table = layer_config["primary_keys"]
    target_tables = list(primary_keys_by_table.keys())
    if args.tables:
        target_tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    endpoint_url = resolve_endpoint(os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "password123"),
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    print("Connecting to ClickHouse and ensuring database exists...")
    execute_clickhouse_query(f"CREATE DATABASE IF NOT EXISTS {ch_db}")
    print(f"Layer: {args.layer}")
    print(f"Delta base path: {base_path}")
    print(f"ClickHouse database: {ch_db}")
    
    for table_name in target_tables:
        print(f"\nProcessing table: {table_name}")
        pk_list = primary_keys_by_table.get(table_name)
        if not pk_list:
            print(f"❌ No primary key configured for table '{table_name}'. Skipping.")
            continue

        s3_path = f"{base_path.rstrip('/')}/{table_name}".replace("s3a://", "s3://")
        
        try:
            dt = DeltaTable(s3_path, storage_options=storage_options)
            schema = dt.schema()
            
            # Build column list
            columns_ddl = []
            
            for field in schema.fields:
                ch_type = map_type_to_clickhouse(field.name, field.type)
                
                # Check if column is a primary key or the version column (updated_at).
                # In ClickHouse ReplacingMergeTree, order by fields and version fields cannot be Nullable.
                if field.name in pk_list or field.name == 'updated_at':
                    columns_ddl.append(f"    `{field.name}` {ch_type}")
                else:
                    columns_ddl.append(f"    `{field.name}` Nullable({ch_type})")
                    
            columns_str = ",\n".join(columns_ddl)
            pk_str = ", ".join([f"`{pk}`" for pk in pk_list])
            
            # Standard clickhouse DDL using ReplacingMergeTree
            # We order by the primary key.
            # If the table has an updated_at column, we can use it to determine the latest version.
            has_updated_at = any(f.name == 'updated_at' for f in schema.fields)
            
            if has_updated_at:
                engine_str = f"ReplacingMergeTree(`updated_at`)"
            else:
                engine_str = "ReplacingMergeTree()"
                
            ddl = f"""
CREATE TABLE IF NOT EXISTS {ch_db}.{table_name}
(
{columns_str}
)
ENGINE = {engine_str}
ORDER BY ({pk_str})
"""
            print(f"Generated DDL for {table_name}:")
            print(ddl)
            
            if args.recreate:
                print(f"Dropping existing ClickHouse table {ch_db}.{table_name} before recreate...")
                execute_clickhouse_query(f"DROP TABLE IF EXISTS {ch_db}.{table_name}", database=ch_db)

            print(f"Executing DDL for {table_name} in ClickHouse...")
            execute_clickhouse_query(ddl, database=ch_db)
            print(f"✔ Table {table_name} created successfully!")
            
        except Exception as e:
            print(f"❌ Error processing {table_name}: {e}")

if __name__ == "__main__":
    main()
