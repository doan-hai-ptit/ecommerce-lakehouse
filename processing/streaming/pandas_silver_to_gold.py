import argparse
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
# Ensure parent processing/ directory is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pandas_hive_utils import sync_hive_delta_table

# Load environment variables
load_dotenv()

def parse_args():
    parser = argparse.ArgumentParser(
        description="Process Silver Delta tables to Gold dimension/fact tables using Pandas & deltalake."
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("SILVER_BASE_PATH", "s3a://silver-lakehouse"),
        help="Silver bucket/base path.",
    )
    parser.add_argument(
        "--gold-base",
        default=os.getenv("GOLD_BASE_PATH", "s3a://gold-lakehouse"),
        help="Gold bucket/base path.",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Optional comma-separated target dimension/fact tables list.",
    )
    parser.add_argument(
        "--hive-db",
        default=os.getenv("GOLD_HIVE_DATABASE", "gold"),
        help="Hive database name for Gold tables.",
    )
    parser.add_argument(
        "--skip-hive-sync",
        action="store_true",
        default=os.getenv("GOLD_SKIP_HIVE_SYNC", "false").lower() == "true",
        help="Skip syncing Gold tables metadata to Hive Metastore.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Processing check interval in seconds. Default: 10.0.",
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

def read_silver_pandas(silver_base, table_name, storage_options):
    path = f"{silver_base.rstrip('/')}/{table_name}".replace("s3a://", "s3://")
    try:
        dt = DeltaTable(path, storage_options=storage_options)
        return dt.to_pyarrow_table().to_pandas()
    except TableNotFoundError:
        return None
    except Exception as e:
        print(f"Error reading Silver table '{table_name}': {e}")
        return None

# ==========================================
# Gold Table Builders
# ==========================================

def build_dim_platforms(silver_base, storage_options):
    platforms = read_silver_pandas(silver_base, "platforms", storage_options)
    if platforms is None or platforms.empty:
        return None
    return platforms[[
        "platform_id", "platform_code", "platform_name", "base_url", "is_active", "created_at", "updated_at"
    ]]

def build_dim_brands(silver_base, storage_options):
    brands = read_silver_pandas(silver_base, "brands", storage_options)
    platforms = read_silver_pandas(silver_base, "platforms", storage_options)
    if brands is None or brands.empty:
        return None
        
    if platforms is not None and not platforms.empty:
        joined = pd.merge(brands, platforms, on="platform_id", how="left", suffixes=("", "_platform"))
        joined = joined.rename(columns={"created_at_platform": "platform_created_at"})
    else:
        joined = brands.copy()
        joined["platform_code"] = None
        joined["platform_name"] = None
        
    return joined[[
        "brand_id", "platform_id", "platform_code", "platform_name", "platform_brand_id",
        "brand_name", "country", "is_official", "created_at", "updated_at"
    ]]

def build_dim_sellers(silver_base, storage_options):
    sellers = read_silver_pandas(silver_base, "sellers", storage_options)
    platforms = read_silver_pandas(silver_base, "platforms", storage_options)
    if sellers is None or sellers.empty:
        return None
        
    if platforms is not None and not platforms.empty:
        joined = pd.merge(sellers, platforms, on="platform_id", how="left", suffixes=("", "_platform"))
    else:
        joined = sellers.copy()
        joined["platform_code"] = None
        joined["platform_name"] = None
        
    return joined[[
        "seller_id", "platform_id", "platform_code", "platform_name", "platform_seller_id",
        "seller_name", "city", "province", "follower_count", "is_official_store", "status",
        "created_at", "updated_at"
    ]]

def build_dim_customers(silver_base, storage_options):
    customers = read_silver_pandas(silver_base, "customers", storage_options)
    platforms = read_silver_pandas(silver_base, "platforms", storage_options)
    addresses = read_silver_pandas(silver_base, "customer_addresses", storage_options)
    if customers is None or customers.empty:
        return None
        
    primary_addr = None
    if addresses is not None and not addresses.empty:
        # Sort by default address and latest update, keep the first for each customer
        addresses_sorted = addresses.sort_values(by=["is_default", "updated_at"], ascending=[False, False])
        primary_addr = addresses_sorted.drop_duplicates(subset=["customer_id"], keep="first")
        primary_addr = primary_addr.rename(columns={
            "address_line": "primary_address_line",
            "ward": "primary_ward",
            "district": "primary_district",
            "city": "primary_city",
            "province": "primary_province",
            "country": "primary_country"
        })[[
            "customer_id", "primary_address_line", "primary_ward", "primary_district",
            "primary_city", "primary_province", "primary_country"
        ]]
        
    joined = pd.merge(customers, platforms, on="platform_id", how="left", suffixes=("", "_platform")) if platforms is not None else customers.copy()
    if platforms is None:
        joined["platform_code"] = None
        joined["platform_name"] = None
        
    if primary_addr is not None:
        joined = pd.merge(joined, primary_addr, on="customer_id", how="left")
    else:
        for col in ["primary_address_line", "primary_ward", "primary_district", "primary_city", "primary_province", "primary_country"]:
            joined[col] = None
            
    return joined[[
        "customer_id", "platform_id", "platform_code", "platform_name", "platform_customer_id",
        "full_name", "email", "phone_number", "gender", "date_of_birth", "status",
        "primary_address_line", "primary_ward", "primary_district", "primary_city",
        "primary_province", "primary_country", "created_at", "updated_at"
    ]]

def build_dim_products(silver_base, storage_options):
    products = read_silver_pandas(silver_base, "products", storage_options)
    sellers = read_silver_pandas(silver_base, "sellers", storage_options)
    categories = read_silver_pandas(silver_base, "categories", storage_options)
    brands = read_silver_pandas(silver_base, "brands", storage_options)
    if products is None or products.empty:
        return None
        
    joined = products.copy()
    if sellers is not None and not sellers.empty:
        joined = pd.merge(joined, sellers[["seller_id", "seller_name"]], on="seller_id", how="left")
    else:
        joined["seller_name"] = None
        
    if categories is not None and not categories.empty:
        joined = pd.merge(joined, categories[["category_id", "category_name"]], on="category_id", how="left")
    else:
        joined["category_name"] = None
        
    if brands is not None and not brands.empty:
        joined = pd.merge(joined, brands[["brand_id", "brand_name"]], on="brand_id", how="left")
    else:
        joined["brand_name"] = None
        
    return joined[[
        "product_id", "platform_product_id", "seller_id", "seller_name", "category_id",
        "category_name", "brand_id", "brand_name", "product_name", "description", "status",
        "is_authentic", "published_at", "created_at", "updated_at"
    ]]

def build_dim_product_variants(silver_base, storage_options):
    variants = read_silver_pandas(silver_base, "product_variants", storage_options)
    products = read_silver_pandas(silver_base, "products", storage_options)
    if variants is None or variants.empty:
        return None
        
    if products is not None and not products.empty:
        joined = pd.merge(variants, products[["product_id", "product_name"]], on="product_id", how="left")
    else:
        joined = variants.copy()
        joined["product_name"] = None
        
    return joined[[
        "variant_id", "product_id", "product_name", "platform_variant_id", "sku",
        "variant_name", "original_price", "sale_price", "weight_gram", "status",
        "created_at", "updated_at"
    ]]

def build_dim_date():
    start_date = "2025-01-01"
    end_date = "2027-12-31"
    dates = pd.date_range(start=start_date, end=end_date)
    df = pd.DataFrame({"date_actual": dates})
    
    df["date_key"] = df["date_actual"].dt.strftime("%Y%m%dd").str.replace("d", "").astype(int)
    df["day_of_week"] = df["date_actual"].dt.dayofweek + 1
    df["day_name"] = df["date_actual"].dt.strftime("%A")
    df["month"] = df["date_actual"].dt.month
    df["month_name"] = df["date_actual"].dt.strftime("%B")
    df["quarter"] = df["date_actual"].dt.quarter
    df["year"] = df["date_actual"].dt.year
    df["is_weekend"] = df["day_of_week"].isin([1, 7])
    
    return df[[
        "date_key", "date_actual", "day_of_week", "day_name", "month", "month_name",
        "quarter", "year", "is_weekend"
    ]]

def build_fct_orders(silver_base, storage_options):
    orders = read_silver_pandas(silver_base, "orders", storage_options)
    sellers = read_silver_pandas(silver_base, "sellers", storage_options)
    if orders is None or orders.empty:
        return None
        
    if sellers is not None and not sellers.empty:
        joined = pd.merge(orders, sellers[["seller_id", "platform_id"]], on="seller_id", how="left")
    else:
        joined = orders.copy()
        joined["platform_id"] = None
        
    joined["date_key"] = pd.to_datetime(joined["ordered_at"]).dt.strftime("%Y%m%d").astype(int)
    joined = joined.rename(columns={"ordered_at": "created_at"})
    
    return joined[[
        "order_id", "platform_order_id", "customer_id", "seller_id", "platform_id", "date_key",
        "order_status", "subtotal_amount", "shipping_fee", "discount_amount", "total_amount",
        "created_at", "updated_at"
    ]]

def build_fct_order_items(silver_base, storage_options):
    order_items = read_silver_pandas(silver_base, "order_items", storage_options)
    orders = read_silver_pandas(silver_base, "orders", storage_options)
    variants = read_silver_pandas(silver_base, "product_variants", storage_options)
    sellers = read_silver_pandas(silver_base, "sellers", storage_options)
    
    if order_items is None or order_items.empty or orders is None or orders.empty:
        return None
        
    orders_sel = orders[["order_id", "customer_id", "seller_id", "shipping_fee", "ordered_at", "updated_at"]]
    joined = pd.merge(order_items, orders_sel, on="order_id", how="inner")
    
    if sellers is not None and not sellers.empty:
        joined = pd.merge(joined, sellers[["seller_id", "platform_id"]], on="seller_id", how="left")
    else:
        joined["platform_id"] = None
        
    if variants is not None and not variants.empty:
        joined = pd.merge(joined, variants[["variant_id", "product_id"]], on="variant_id", how="left")
    else:
        joined["product_id"] = None
        
    joined["date_key"] = pd.to_datetime(joined["ordered_at"]).dt.strftime("%Y%m%d").astype(int)
    joined["net_amount"] = (joined["unit_price"] * joined["quantity"]) - joined["discount_amount"]
    
    joined = joined.rename(columns={
        "ordered_at": "created_at",
        "updated_at_x": "updated_at", # item level updated_at or fallback
    })
    
    # Fallback if rename column doesn't match
    if "updated_at" not in joined.columns:
        joined["updated_at"] = joined["created_at"]
        
    return joined[[
        "order_item_id", "order_id", "platform_id", "seller_id", "customer_id", "variant_id",
        "product_id", "date_key", "quantity", "unit_price", "discount_amount", "shipping_fee",
        "net_amount", "created_at", "updated_at"
    ]]

def build_fct_product_reviews(silver_base, storage_options):
    reviews = read_silver_pandas(silver_base, "product_reviews", storage_options)
    if reviews is None or reviews.empty:
        return None
    
    joined = reviews.rename(columns={"reviewed_at": "created_at"})
    return joined[[
        "review_id", "platform_review_id", "product_id", "order_item_id", "customer_id",
        "rating", "title", "content", "delivery_rating", "seller_rating", "helpful_count",
        "status", "created_at", "updated_at"
    ]]

def build_fct_shipments(silver_base, storage_options):
    shipments = read_silver_pandas(silver_base, "shipments", storage_options)
    orders = read_silver_pandas(silver_base, "orders", storage_options)
    sellers = read_silver_pandas(silver_base, "sellers", storage_options)
    
    if shipments is None or shipments.empty:
        return None
        
    joined = shipments.copy()
    if orders is not None and not orders.empty:
        joined = pd.merge(joined, orders[["order_id", "customer_id", "seller_id", "ordered_at"]], on="order_id", how="left")
        if sellers is not None and not sellers.empty:
            joined = pd.merge(joined, sellers[["seller_id", "platform_id"]], on="seller_id", how="left")
        else:
            joined["platform_id"] = None
    else:
        joined["customer_id"] = None
        joined["seller_id"] = None
        joined["platform_id"] = None
        joined["ordered_at"] = None
        
    # date_key calculation
    date_col = joined["shipped_at"].fillna(joined["created_at"])
    joined["date_key"] = pd.to_datetime(date_col).dt.strftime("%Y%m%d").astype(int)

    ordered_at = pd.to_datetime(joined["ordered_at"], errors="coerce")
    delivered_at = pd.to_datetime(joined["delivered_at"], errors="coerce")
    estimated_delivery_at = pd.to_datetime(joined["estimated_delivery_at"], errors="coerce")

    joined["delivery_duration_hours"] = (
        (delivered_at - ordered_at).dt.total_seconds() / 3600.0
    )
    joined["is_delayed"] = (
        delivered_at.notna()
        & estimated_delivery_at.notna()
        & (delivered_at > estimated_delivery_at)
    ).astype(int)
    
    return joined[[
        "shipment_id", "order_id", "platform_id", "seller_id", "customer_id", "date_key",
        "carrier_name", "tracking_number", "shipping_method", "status", "shipped_at",
        "estimated_delivery_at", "delivered_at", "delivery_duration_hours", "is_delayed",
        "created_at", "updated_at"
    ]]

# ==========================================
# Main Orchestration Loop
# ==========================================

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

PRIMARY_KEYS = {
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

def merge_to_gold(df, table_name, target_path, storage_options):
    if df is None or df.empty:
        return
        
    s3_path = target_path.replace("s3a://", "s3://")
    primary_keys = PRIMARY_KEYS.get(table_name)
    
    try:
        dt = DeltaTable(s3_path, storage_options=storage_options)
        
        predicate = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
        updates = {col: f"source.{col}" for col in df.columns}
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Merging into Gold Table: {s3_path}")
        dt.merge(
            source=df,
            predicate=predicate,
            source_alias="source",
            target_alias="target"
        ) \
        .when_matched_update(updates=updates) \
        .when_not_matched_insert(updates=updates) \
        .execute()
        
    except TableNotFoundError:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Initializing Gold Table: {s3_path}")
        write_deltalake(
            s3_path,
            df,
            mode="append",
            storage_options=storage_options
        )

def main():
    args = parse_args()
    storage_options = get_storage_options()
    
    allowed_tables = set(BUILDERS.keys())
    if args.tables:
        allowed_tables = {t.strip() for t in args.tables.split(",") if t.strip()}
        
    print(f"Silver to Gold Dimension/Fact processor started (Interval: {args.interval}s)")
    print(f"Silver Base: {args.silver_base} -> Gold Base: {args.gold_base}")
    if not args.skip_hive_sync:
        print(f"Hive Metastore DB: {args.hive_db}")
    
    try:
        while True:
            # 1. Process static/dynamic dim_date first if requested
            if "dim_date" in allowed_tables or args.tables is None:
                try:
                    dim_date_df = build_dim_date()
                    gold_path = f"{args.gold_base.rstrip('/')}/dim_date"
                    merge_to_gold(dim_date_df, "dim_date", gold_path, storage_options)
                    
                    if not args.skip_hive_sync:
                        try:
                            sync_hive_delta_table(
                                args.hive_db,
                                "dim_date",
                                gold_path,
                                storage_options=storage_options
                            )
                        except Exception as e:
                            print(f"Error syncing dim_date to Hive: {e}")
                except Exception as e:
                    print(f"Error building dim_date: {e}")
            
            # 2. Process dimensions and facts
            for table_name in sorted(allowed_tables):
                builder_func = BUILDERS.get(table_name)
                if not builder_func:
                    continue
                    
                gold_path = f"{args.gold_base.rstrip('/')}/{table_name}"
                try:
                    gold_df = builder_func(args.silver_base, storage_options)
                    if gold_df is not None and not gold_df.empty:
                        # Deduplicate gold by primary key, keeping latest based on updated_at
                        primary_keys = PRIMARY_KEYS[table_name]
                        if "updated_at" in gold_df.columns:
                            gold_df = gold_df.sort_values(by="updated_at", ascending=True)
                        gold_df = gold_df.drop_duplicates(subset=primary_keys, keep="last")
                        
                        merge_to_gold(gold_df, table_name, gold_path, storage_options)
                        
                        if not args.skip_hive_sync:
                            try:
                                sync_hive_delta_table(
                                    args.hive_db,
                                    table_name,
                                    gold_path,
                                    storage_options=storage_options
                                )
                            except Exception as e:
                                print(f"Error syncing {table_name} to Hive: {e}")
                except Exception as e:
                    print(f"Error processing Gold table '{table_name}': {e}")
                    
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\nStopping Silver to Gold processor...")

if __name__ == "__main__":
    main()
