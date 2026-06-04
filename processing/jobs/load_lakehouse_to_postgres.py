import os
import sys
import argparse

# Add parent directory to path so we can import core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from core.spark_session import get_spark_session

load_dotenv()

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load Silver or Gold Delta tables from MinIO to target schemas in PostgreSQL database."
    )
    parser.add_argument(
        "--layer",
        choices=["silver", "gold", "both"],
        default="silver",
        help="Select which data layer to upload: silver, gold, or both (default: silver)."
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("SILVER_BASE_PATH", "s3a://silver-lakehouse"),
        help="Silver base path on MinIO/S3."
    )
    parser.add_argument(
        "--gold-base",
        default=os.getenv("GOLD_BASE_PATH", "s3a://gold-lakehouse"),
        help="Gold base path on MinIO/S3."
    )
    return parser.parse_args()

def get_db_properties():
    # Load remote DB config from .env file
    db_host = os.getenv("REMOTE_DB_HOST")
    db_port = os.getenv("REMOTE_DB_PORT")
    db_user = os.getenv("REMOTE_DB_USER")
    db_password = os.getenv("REMOTE_DB_PASSWORD")
    db_name = os.getenv("REMOTE_DB_DATABASE")

    url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }
    return url, properties

def ensure_schema(spark, url, properties, schema):
    print(f"📦 Ensuring schema '{schema}' exists in target database...")
    try:
        jvm = spark.sparkContext._gateway.jvm
        conn = jvm.java.sql.DriverManager.getConnection(url, properties["user"], properties["password"])
        stmt = conn.createStatement()
        stmt.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        stmt.close()
        conn.close()
        print(f"  ✓ Schema '{schema}' is ready.")
    except Exception as e:
        print(f"  ⚠️ Could not create schema '{schema}': {e}")

def check_table_exists(spark, url, properties, schema, table):
    try:
        jvm = spark.sparkContext._gateway.jvm
        conn = jvm.java.sql.DriverManager.getConnection(url, properties["user"], properties["password"])
        stmt = conn.createStatement()
        stmt.executeQuery(f'SELECT 1 FROM "{schema}"."{table}" LIMIT 1')
        stmt.close()
        conn.close()
        return True
    except Exception:
        return False

def truncate_tables(spark, url, properties, schema, tables):
    print(f"🧹 Emptying target tables in schema '{schema}' to prepare for topological load...")
    try:
        jvm = spark.sparkContext._gateway.jvm
        conn = jvm.java.sql.DriverManager.getConnection(url, properties["user"], properties["password"])
        stmt = conn.createStatement()
        
        # Only truncate tables that actually exist in the database schema
        existing_tables = [t for t in tables if check_table_exists(spark, url, properties, schema, t)]
        if existing_tables:
            table_list = ", ".join([f'"{schema}"."{t}"' for t in existing_tables])
            # Use CASCADE to automatically clean up dependent foreign rows
            stmt.execute(f"TRUNCATE TABLE {table_list} CASCADE")
            print("  ✓ Tables emptied successfully (CASCADE).")
        else:
            print("  ✓ No existing tables to empty.")
        stmt.close()
        conn.close()
    except Exception as e:
        print(f"  ⚠️ Truncate failed: {e}")

def load_table(spark, url, properties, source_path, schema, table):
    target_table_name = f'"{schema}"."{table}"'
    print(f"🔄 Loading {source_path} into {target_table_name}...")
    
    exists = check_table_exists(spark, url, properties, schema, table)
    
    try:
        df = spark.read.format("delta").load(source_path)
        
        # Drop partitioning columns (event_date, partition_date) if present in DataFrame 
        # but not part of target database relational columns.
        for col_to_drop in ["event_date", "partition_date"]:
            if col_to_drop in df.columns:
                df = df.drop(col_to_drop)

        writer = (
            df.write
            .format("jdbc")
            .option("url", url)
            .option("dbtable", f"{schema}.{table}")
            .option("user", properties["user"])
            .option("password", properties["password"])
            .option("driver", properties["driver"])
        )
        
        if exists:
            # We already truncated all tables at the start of the job. 
            # We write with 'append' mode to safely insert rows under existing schema constraints.
            writer = writer.mode("append")
            print(f"  → Table exists. Appending to pre-truncated table.")
        else:
            # Table does not exist, let Spark auto-create it
            writer = writer.mode("overwrite")
            print(f"  → Table does not exist in schema '{schema}'. Creating new table using Spark types.")
            
        writer.save()
        print(f"  ✓ Successfully uploaded {target_table_name}")
    except Exception as e:
        print(f"  ❌ Failed to upload {target_table_name}: {e}")

def main():
    args = parse_args()
    spark = get_spark_session(app_name="LakehouseToPostgres", enable_hive_support=False)
    url, properties = get_db_properties()

    # Define tables in safe topological/insertion order to minimize constraint conflicts
    silver_tables = [
        "platforms",
        "sellers",
        "customers",
        "customer_addresses",
        "categories",
        "brands",
        "products",
        "product_variants",
        "product_inventory",
        "inventory_movements",
        "vouchers",
        "carts",
        "cart_items",
        "orders",
        "order_items",
        "payments",
        "shipments",
        "product_reviews",
        "events"
    ]

    gold_tables = [
        "dim_platforms",
        "dim_brands",
        "dim_sellers",
        "dim_customers",
        "dim_products",
        "dim_product_variants",
        "dim_date",
        "fct_orders",
        "fct_order_items",
        "fct_product_reviews",
        "fct_shipments"
    ]

    try:
        if args.layer in ["silver", "both"]:
            print("\n=== UPLOADING SILVER LAYER ===")
            ensure_schema(spark, url, properties, "silver")
            truncate_tables(spark, url, properties, "silver", silver_tables)
            for table in silver_tables:
                source_path = f"{args.silver_base.rstrip('/')}/{table}"
                load_table(spark, url, properties, source_path, "silver", table)

        if args.layer in ["gold", "both"]:
            print("\n=== UPLOADING GOLD LAYER ===")
            ensure_schema(spark, url, properties, "gold")
            truncate_tables(spark, url, properties, "gold", gold_tables)
            for table in gold_tables:
                source_path = f"{args.gold_base.rstrip('/')}/{table}"
                load_table(spark, url, properties, source_path, "gold", table)

        print("\n✅ All requested uploads to schemas completed successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
