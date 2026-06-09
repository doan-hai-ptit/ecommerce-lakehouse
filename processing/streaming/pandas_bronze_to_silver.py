import argparse
import json
import os
import sys
import time
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import boto3
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

# Ensure parent processing/ directory is in sys.path so we can import schemas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Check if pyspark is installed; if not, mock it to allow importing schemas.py without errors
try:
    import pyspark
    from pyspark.sql.types import (
        IntegerType, LongType, StringType, BooleanType,
        DecimalType, DateType, TimestampType
    )
except ModuleNotFoundError:
    import sys
    from types import ModuleType
    
    # Create mock classes dynamically to retain their original names
    pyspark_mock = ModuleType("pyspark")
    sql_mock = ModuleType("pyspark.sql")
    types_mock = ModuleType("pyspark.sql.types")
    functions_mock = ModuleType("pyspark.sql.functions")
    
    # Base Mock Class
    class MockClass:
        def __init__(self, *args, **kwargs): pass
        
    setattr(sql_mock, "DataFrame", MockClass)
    setattr(sql_mock, "SparkSession", MockClass)
    setattr(sql_mock, "Window", MockClass)
    
    for name in ["IntegerType", "LongType", "StringType", "BooleanType", "DecimalType", "DateType", "TimestampType", "StructType", "StructField"]:
        mock_class = type(name, (object,), {"__init__": lambda self, *args, **kwargs: None})
        setattr(types_mock, name, mock_class)
        
    sql_mock.types = types_mock
    sql_mock.functions = functions_mock
    pyspark_mock.sql = sql_mock
    
    sys.modules["pyspark"] = pyspark_mock
    sys.modules["pyspark.sql"] = sql_mock
    sys.modules["pyspark.sql.types"] = types_mock
    sys.modules["pyspark.sql.functions"] = functions_mock
    
    # Mock delta framework since __init__.py loads orchestrator -> writer which loads delta.tables
    delta_mock = ModuleType("delta")
    delta_tables_mock = ModuleType("delta.tables")
    setattr(delta_tables_mock, "DeltaTable", MockClass)
    delta_mock.tables = delta_tables_mock
    sys.modules["delta"] = delta_mock
    sys.modules["delta.tables"] = delta_tables_mock
    
    # Expose the mock classes locally
    IntegerType = getattr(types_mock, "IntegerType")
    LongType = getattr(types_mock, "LongType")
    StringType = getattr(types_mock, "StringType")
    BooleanType = getattr(types_mock, "BooleanType")
    DecimalType = getattr(types_mock, "DecimalType")
    DateType = getattr(types_mock, "DateType")
    TimestampType = getattr(types_mock, "TimestampType")

from streaming.bronze_to_silver.schemas import TABLE_SPECS

# Load environment variables
load_dotenv()

def parse_args():
    parser = argparse.ArgumentParser(
        description="Process Bronze Delta events to Silver Delta tables incrementally using Pandas & deltalake."
    )
    parser.add_argument(
        "--bronze-path",
        default=os.getenv("KAFKA_BRONZE_PATH", "s3a://bronze-lakehouse/kafka_cdc"),
        help="Bronze Delta path.",
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("SILVER_BASE_PATH", "s3a://silver-lakehouse"),
        help="Silver bucket/base path.",
    )
    parser.add_argument(
        "--tables",
        default=os.getenv("BRONZE_TO_SILVER_TABLES"),
        help="Optional comma-separated source_table allow-list.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Incremental processing check interval in seconds. Default: 5.0.",
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

def get_s3_client():
    endpoint_url = resolve_endpoint(os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password123")
    
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

def parse_s3_url(url):
    # Normalize s3a:// or s3:// to standard parsing
    url_norm = url.replace("s3a://", "s3://")
    parsed = urlparse(url_norm)
    return parsed.netloc, parsed.path.lstrip("/")

def get_last_processed_offset(s3_client, bucket, table_name):
    key = f"_checkpoints/pandas/bronze_to_silver/{table_name}.json"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data.get("last_processed_offset", -1)
    except Exception:
        return -1

def save_last_processed_offset(s3_client, bucket, table_name, offset):
    key = f"_checkpoints/pandas/bronze_to_silver/{table_name}.json"
    try:
        data = json.dumps({"last_processed_offset": offset, "updated_at": datetime.now().isoformat()})
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    except Exception as e:
        print(f"Failed to save checkpoint for {table_name}: {e}")

def cast_column(series, data_type):
    if isinstance(data_type, StringType):
        trimmed = series.astype(str).str.strip()
        return trimmed.where(trimmed.str.len() > 0, None)
        
    if isinstance(data_type, BooleanType):
        return series.map(
            lambda x: True if str(x).lower() in ("true", "1") else (False if str(x).lower() in ("false", "0") else None)
        ).astype("boolean")
        
    if isinstance(data_type, (IntegerType, LongType)):
        return pd.to_numeric(series, errors="coerce").astype("Int64")
        
    if isinstance(data_type, DecimalType):
        return pd.to_numeric(series, errors="coerce")
        
    if isinstance(data_type, DateType):
        def parse_date(x):
            if pd.isna(x):
                return None
            val = str(x)
            if val.isdigit() or (val.startswith("-") and val[1:].isdigit()):
                return pd.Timestamp("1970-01-01").date() + pd.to_timedelta(int(val), unit="D")
            return pd.to_datetime(val, errors="coerce").date()
        return series.apply(parse_date)
        
    if isinstance(data_type, TimestampType):
        def parse_timestamp(x):
            if pd.isna(x):
                return None
            val = str(x)
            if val.replace(".", "", 1).isdigit() or (val.startswith("-") and val[1:].replace(".", "", 1).isdigit()):
                numeric_val = float(val)
                if abs(numeric_val) >= 1e15:
                    return pd.to_datetime(numeric_val, unit="us")
                elif abs(numeric_val) >= 1e12:
                    return pd.to_datetime(numeric_val, unit="ms")
                else:
                    return pd.to_datetime(numeric_val, unit="s")
            return pd.to_datetime(val, errors="coerce")
        return series.apply(parse_timestamp)
        
    return series

def transform_bronze_to_silver(bronze_df, spec):
    # Choose payload_before for deletes, payload_after for others
    payload_strings = bronze_df["payload_after"].where(
        bronze_df["debezium_op"] != "d", 
        bronze_df["payload_before"]
    )
    
    transformed_records = []
    
    # Parse payload JSON fields
    for idx, raw_json in enumerate(payload_strings):
        parsed_record = {}
        if pd.notna(raw_json):
            try:
                parsed_record = json.loads(raw_json)
            except Exception:
                pass
        
        row_data = {}
        for col_name, _ in spec.columns:
            row_data[col_name] = parsed_record.get(col_name) if parsed_record else None
            
        row_data["event_date"] = bronze_df.iloc[idx]["event_date"]
        row_data["_change_op"] = bronze_df.iloc[idx]["debezium_op"] if pd.notna(bronze_df.iloc[idx]["debezium_op"]) else "r"
        row_data["_kafka_timestamp"] = bronze_df.iloc[idx]["kafka_timestamp"]
        row_data["_kafka_offset"] = int(bronze_df.iloc[idx]["offset"])
        
        transformed_records.append(row_data)
        
    df = pd.DataFrame(transformed_records)
    if df.empty:
        return df
        
    # Cast columns to their specification types
    for col_name, data_type in spec.columns:
        df[col_name] = cast_column(df[col_name], data_type)
        
    # Deduplicate within this batch, keeping the latest by offset
    df = df.sort_values(by=["_kafka_offset"], ascending=True)
    df = df.drop_duplicates(subset=spec.primary_keys, keep="last")
    return df

def merge_to_silver(df, table_name, spec, target_path, storage_options):
    if df.empty:
        return
        
    s3_path = target_path.replace("s3a://", "s3://")
    
    # Make sure we drop internal columns before merging
    columns_to_write = [c for c, _ in spec.columns] + ["event_date"]
    clean_df = df[columns_to_write]
    
    try:
        dt = DeltaTable(s3_path, storage_options=storage_options)
        
        # Build merge predicates
        predicate = " AND ".join([f"target.{key} = source.{key}" for key in spec.primary_keys])
        updates = {col: f"source.{col}" for col in columns_to_write}
        
        # Perform ACID Merge
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Merging into Silver Table: {s3_path}")
        dt.merge(
            source=clean_df,
            predicate=predicate,
            source_alias="source",
            target_alias="target"
        ) \
        .when_matched_delete(predicate="source._change_op = 'd'") \
        .when_matched_update(predicate="source._change_op != 'd'", updates=updates) \
        .when_not_matched_insert(predicate="source._change_op != 'd'", updates=updates) \
        .execute()
        
    except TableNotFoundError:
        # If the table doesn't exist, we initialize it
        initial_df = df[df["_change_op"] != "d"][columns_to_write]
        if not initial_df.empty:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Initializing Silver Table: {s3_path}")
            write_deltalake(
                s3_path,
                initial_df,
                mode="append",
                partition_by=["event_date"],
                storage_options=storage_options
            )

def process_bronze_table(dt_bronze, table_name, spec, silver_base, s3_client, silver_bucket, storage_options):
    silver_path = f"{silver_base.rstrip('/')}/{table_name}"
    
    # 1. Get the last offset processed in Silver from S3 checkpoint
    max_offset = get_last_processed_offset(s3_client, silver_bucket, table_name)
    
    # 2. Query new data from Bronze Delta Table
    try:
        filters = [
            [("source_table", "==", table_name), ("offset", ">", max_offset)]
        ]
        pyarrow_tbl = dt_bronze.to_pyarrow_table(filters=filters)
        if len(pyarrow_tbl) == 0:
            return
            
        bronze_df = pyarrow_tbl.to_pandas()
        max_batch_offset = int(bronze_df["offset"].max())
        
        print(f"Found {len(bronze_df)} new changes for Silver table '{table_name}' (offsets: {max_offset + 1} to {max_batch_offset})")
        
        # 3. Transform data using Pandas
        silver_df = transform_bronze_to_silver(bronze_df, spec)
        
        # 4. Write/Merge into Silver Table
        merge_to_silver(silver_df, table_name, spec, silver_path, storage_options)
        
        # 5. Save the last processed offset
        save_last_processed_offset(s3_client, silver_bucket, table_name, max_batch_offset)
        
    except Exception as e:
        print(f"Error processing Silver table '{table_name}': {e}")

def main():
    args = parse_args()
    storage_options = get_storage_options()
    s3_client = get_s3_client()
    
    silver_bucket, _ = parse_s3_url(args.silver_base)
    
    # Filter target tables
    allowed_tables = set(TABLE_SPECS.keys())
    if args.tables:
        allowed_tables = {t.strip() for t in args.tables.split(",") if t.strip()}
        
    print(f"Bronze to Silver Incremental processor started (Interval: {args.interval}s)")
    print(f"Bronze path: {args.bronze_path} -> Silver Base: {args.silver_base}")
    
    bronze_s3_path = args.bronze_path.replace("s3a://", "s3://")
    
    try:
        while True:
            try:
                # Load Bronze Delta Table
                dt_bronze = DeltaTable(bronze_s3_path, storage_options=storage_options)
                
                # Check each table incrementally
                for table_name in sorted(allowed_tables):
                    spec = TABLE_SPECS[table_name]
                    process_bronze_table(dt_bronze, table_name, spec, args.silver_base, s3_client, silver_bucket, storage_options)
                    
            except TableNotFoundError:
                print(f"Bronze Delta table at '{bronze_s3_path}' not found yet. Waiting...")
            except Exception as e:
                print(f"Loop error: {e}")
                
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\nStopping Bronze to Silver processor...")

if __name__ == "__main__":
    main()
