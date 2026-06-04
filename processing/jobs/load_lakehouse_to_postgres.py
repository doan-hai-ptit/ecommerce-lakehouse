import os
import sys
import argparse

# Add parent directory to path so we can import core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from core.spark_session import get_spark_session
from streaming.bronze_to_silver.schemas import TABLE_SPECS
from streaming.silver_to_gold.builders import PRIMARY_KEYS as GOLD_PRIMARY_KEYS

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
        "--mode",
        choices=["overwrite", "upsert"],
        default="overwrite",
        help="Load mode: 'overwrite' (truncates target tables first) or 'upsert' (inserts or updates on primary key conflicts)."
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

def load_table(spark, url, properties, source_path, schema, table, mode, primary_keys=None):
    target_table_name = f'"{schema}"."{table}"'
    exists = check_table_exists(spark, url, properties, schema, table)
    
    if mode == "upsert" and exists and primary_keys:
        temp_table_name = f'"{schema}"."temp_staging_{table}"'
        print(f"🔄 [UPSERT] Loading {source_path} into staging {temp_table_name} and merging to {target_table_name}...")
        try:
            df = spark.read.format("delta").load(source_path)
            
            # Drop partitioning columns (event_date, partition_date) if present in DataFrame 
            for col_to_drop in ["event_date", "partition_date"]:
                if col_to_drop in df.columns:
                    df = df.drop(col_to_drop)

            # 1. Write the DataFrame to a temporary staging table
            df.write \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", temp_table_name) \
                .option("user", properties["user"]) \
                .option("password", properties["password"]) \
                .option("driver", properties["driver"]) \
                .mode("overwrite") \
                .save()

            # 2. Execute the UPSERT query on Postgres via JDBC
            jvm = spark.sparkContext._gateway.jvm
            conn = jvm.java.sql.DriverManager.getConnection(url, properties["user"], properties["password"])
            stmt = conn.createStatement()

            # Get columns of the table from the temp table to build the SQL query
            meta = conn.getMetaData()
            rs = meta.getColumns(None, schema, f"temp_staging_{table}", None)
            columns = []
            while rs.next():
                columns.append(rs.getString("COLUMN_NAME"))
            rs.close()

            if not columns:
                raise Exception("No columns found in temp staging table")

            # Perform DELETE of existing matching keys, then INSERT in a single transaction
            # to support target tables without pre-existing UNIQUE constraints in Postgres.
            pks_cond = " AND ".join([f't."{k}" = s."{k}"' for k in primary_keys])
            delete_sql = f"""
                DELETE FROM {target_table_name} t
                USING {temp_table_name} s
                WHERE {pks_cond}
            """

            cols_str = ", ".join([f'"{c}"' for c in columns])
            select_str = ", ".join([f'"{c}"' for c in columns])
            insert_sql = f"""
                INSERT INTO {target_table_name} ({cols_str})
                SELECT {select_str} FROM {temp_table_name}
            """

            conn.setAutoCommit(False)
            try:
                stmt.execute(delete_sql)
                stmt.execute(insert_sql)
                conn.commit()
                print(f"  ✓ Delete-then-Insert executed successfully.")
            except Exception as tx_err:
                conn.rollback()
                raise tx_err
            finally:
                conn.setAutoCommit(True)

            stmt.execute(f"DROP TABLE {temp_table_name}")
            stmt.close()
            conn.close()
            print(f"  ✓ Successfully upserted {target_table_name}")
        except Exception as e:
            print(f"  ❌ Failed to upsert {target_table_name}: {e}")
            # Try to cleanup temp table just in case
            try:
                conn = jvm.java.sql.DriverManager.getConnection(url, properties["user"], properties["password"])
                stmt = conn.createStatement()
                stmt.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                stmt.close()
                conn.close()
            except Exception:
                pass
    else:
        # Standard insert logic (overwrite or append)
        print(f"🔄 [{mode.upper()}] Loading {source_path} into {target_table_name}...")
        try:
            df = spark.read.format("delta").load(source_path)
            
            # Drop partitioning columns (event_date, partition_date) if present in DataFrame 
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
                writer = writer.mode("append")
                print(f"  → Table exists. Appending to target table.")
            else:
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
            print(f"\n=== UPLOADING SILVER LAYER ({args.mode.upper()} MODE) ===")
            ensure_schema(spark, url, properties, "silver")
            if args.mode == "overwrite":
                truncate_tables(spark, url, properties, "silver", silver_tables)
            for table in silver_tables:
                source_path = f"{args.silver_base.rstrip('/')}/{table}"
                pks = TABLE_SPECS[table].primary_keys if table in TABLE_SPECS else None
                load_table(spark, url, properties, source_path, "silver", table, args.mode, pks)

        if args.layer in ["gold", "both"]:
            print(f"\n=== UPLOADING GOLD LAYER ({args.mode.upper()} MODE) ===")
            ensure_schema(spark, url, properties, "gold")
            if args.mode == "overwrite":
                truncate_tables(spark, url, properties, "gold", gold_tables)
            for table in gold_tables:
                source_path = f"{args.gold_base.rstrip('/')}/{table}"
                pks = GOLD_PRIMARY_KEYS[table] if table in GOLD_PRIMARY_KEYS else None
                load_table(spark, url, properties, source_path, "gold", table, args.mode, pks)

        print("\n✅ All requested uploads to schemas completed successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
