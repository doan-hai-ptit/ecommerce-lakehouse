import argparse
import json
import os
import sys
import re
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import boto3
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

# Ensure the parent directory is in sys.path so we can import core modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pandas_hive_utils import sync_hive_delta_table

# Load environment variables
load_dotenv()

# Hardcoded category mapping for Tiki categories as fallback
DEFAULT_TIKI_CATEGORIES = {
    1846: "Laptop - Máy Vi Tính - Linh kiện",
    1520: "Điện Thoại - Máy Tính Bảng",
    1789: "Đồ Chơi - Mẹ & Bé",
    2549: "Đồ Gia Dụng",
    8322: "Nhà Cửa - Đời Sống",
    915: "Thiết Bị Số - Phụ Kiện Số",
}

def load_category_map():
    # Attempt to load from crawler_state.txt at /app/crawler_state.txt or relative paths
    possible_paths = [
        "/app/crawler_state.txt",
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "crawler_state.txt"),
        os.path.join(os.getcwd(), "crawler_state.txt")
    ]
    
    category_map = DEFAULT_TIKI_CATEGORIES.copy()
    
    for path in possible_paths:
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                for cat_id_str, info in state_data.items():
                    if isinstance(info, dict) and "category_name" in info:
                        category_map[int(cat_id_str)] = info["category_name"]
                    elif isinstance(info, (int, str)):
                        category_map[int(cat_id_str)] = str(info)
                print(f"✓ Loaded dynamic category mappings from {path}")
                break
            except Exception as e:
                print(f"⚠️ Warning: Failed to parse crawler state file at {path}: {e}")
                
    return category_map

# Define Explicit PyArrow Schemas for BI-friendly flat tables
PRODUCTS_SCHEMA = pa.schema([
    ("product_id", pa.int64()),
    ("platform_product_id", pa.string()),
    ("product_name", pa.string()),
    ("category_name", pa.string()),
    ("brand_name", pa.string()),
    ("price", pa.float64()),
    ("original_price", pa.float64()),
    ("discount_rate", pa.float64()),
    ("quantity_sold", pa.int64()),
    ("rating_average", pa.float64()),
    ("reviews_count", pa.int64()),
    ("seller_id", pa.int64()),
    ("event_date", pa.string())
])

SELLERS_SCHEMA = pa.schema([
    ("seller_id", pa.int64()),
    ("platform_seller_id", pa.string()),
    ("seller_name", pa.string()),
    ("is_official_store", pa.bool_())
])

REVIEWS_SCHEMA = pa.schema([
    ("review_id", pa.int64()),
    ("platform_review_id", pa.string()),
    ("product_id", pa.int64()),
    ("rating", pa.int64()),
    ("content", pa.string()),
    ("helpful_count", pa.int64()),
    ("reviewed_at", pa.timestamp('us')),
    ("event_date", pa.string())
])

def parse_args():
    parser = argparse.ArgumentParser(
        description="Batch process Tiki raw Bronze JSON files to Silver Delta tables in real_data folder."
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date string to process in YYYY-MM-DD format. Defaults to today's date."
    )
    parser.add_argument(
        "--bronze-bucket",
        default=os.getenv("MINIO_BUCKET_NAME", "bronze-lakehouse"),
        help="Bronze MinIO bucket name."
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("SILVER_BASE_PATH", "s3a://silver-lakehouse/real_data"),
        help="Silver base path on MinIO."
    )
    parser.add_argument(
        "--hive-db",
        default="silver_real",
        help="Hive database name for real data tables."
    )
    parser.add_argument(
        "--skip-hive-sync",
        action="store_true",
        default=os.getenv("SILVER_SKIP_HIVE_SYNC", "false").lower() == "true",
        help="Skip syncing metadata to Hive Metastore."
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean target Delta tables before writing (useful to fix corrupt tables from failed runs)."
    )
    return parser.parse_args()

def resolve_endpoint(endpoint_url):
    import socket
    parsed = urlparse(endpoint_url)
    if parsed.hostname == "minio":
        try:
            socket.gethostbyname("minio")
        except socket.gaierror:
            new_netloc = parsed.netloc.replace("minio", "localhost")
            endpoint_url = parsed._replace(netloc=new_netloc).geturl()
    return endpoint_url

def get_storage_options():
    endpoint_url = resolve_endpoint(os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"))
    return {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "password123"),
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

def get_s3_client():
    endpoint_url = resolve_endpoint(os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"))
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password123")
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

def safe_int(val, default=0):
    try:
        if val is None or pd.isna(val):
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default

def safe_float(val, default=0.0):
    try:
        if val is None or pd.isna(val):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_str(val, default=""):
    if val is None or pd.isna(val):
        return default
    return str(val).strip()

def safe_datetime(val):
    if val is None or pd.isna(val):
        return None
    try:
        val_str = str(val).strip()
        if not val_str:
            return None
            
        # Check if it is a Unix timestamp (integer or float as string)
        if val_str.replace(".", "", 1).isdigit() or (val_str.startswith("-") and val_str[1:].replace(".", "", 1).isdigit()):
            num_val = float(val_str)
            if abs(num_val) >= 1e15:
                return pd.to_datetime(num_val, unit="us")
            elif abs(num_val) >= 1e12:
                return pd.to_datetime(num_val, unit="ms")
            else:
                return pd.to_datetime(num_val, unit="s")
                
        return pd.to_datetime(val)
    except Exception:
        return None

def clean_s3_folder(s3_client, bucket, prefix):
    print(f"🧹 Cleaning S3 folder: s3://{bucket}/{prefix}")
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            objects = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
            if objects:
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
                print(f"  ✓ Deleted {len(objects)} files in prefix {prefix}")
    except Exception as e:
        print(f"  ⚠️ Error cleaning S3 folder at {prefix}: {e}")

def parse_s3_url(url):
    url_norm = url.replace("s3a://", "s3://")
    parsed = urlparse(url_norm)
    return parsed.netloc, parsed.path.lstrip("/")

def process_tiki_batch(date_str, bronze_bucket, silver_base, hive_db, skip_hive_sync, clean=False):
    s3_client = get_s3_client()
    storage_options = get_storage_options()
    category_map = load_category_map()
    
    print(f"\n⚡ Processing batch for date: {date_str}")
    print(f"  - Bronze bucket: {bronze_bucket}")
    print(f"  - Silver base path: {silver_base}")
    print(f"  - Target Hive Database: {hive_db}")

    if clean:
        silver_bucket, base_prefix = parse_s3_url(silver_base)
        print(f"🧹 Performing clean/re-initialization of target tables under s3://{silver_bucket}/{base_prefix}")
        for tab in ["products", "sellers", "product_reviews"]:
            clean_s3_folder(s3_client, silver_bucket, f"{base_prefix.rstrip('/')}/{tab}/")

    # ==========================================
    # 1. PROCESS PRODUCTS AND SELLERS
    # ==========================================
    prod_prefix = f"provider=tiki/date={date_str}/category=products/"
    print(f"\n🔍 Searching for raw products with prefix: {prod_prefix}")
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bronze_bucket, Prefix=prod_prefix)
        
        raw_products_list = []
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('.json'):
                    continue
                try:
                    response = s3_client.get_object(Bucket=bronze_bucket, Key=key)
                    file_content = json.loads(response['Body'].read().decode('utf-8'))
                    # The content is a list of products
                    if isinstance(file_content, list):
                        raw_products_list.extend(file_content)
                    elif isinstance(file_content, dict) and 'data' in file_content:
                        raw_products_list.extend(file_content['data'])
                except Exception as e:
                    print(f"  ⚠️ Error reading file {key}: {e}")
        
        print(f"  - Found {len(raw_products_list)} raw product records in Bronze.")
        
        if raw_products_list:
            # Parse products
            parsed_products = []
            parsed_sellers = []
            seen_sellers = set()

            for item in raw_products_list:
                p_id = safe_int(item.get('id'))
                if not p_id:
                    continue
                
                # Category lookup
                cat_id = safe_int(item.get('category_id'))
                cat_name = category_map.get(cat_id, "Tiki Category")
                
                # Brand extraction
                brand = item.get('brand_name') or (item.get('brand') or {}).get('name')
                brand_name = safe_str(brand, "Generic")
                
                # Seller extraction
                current_seller = item.get('current_seller') or {}
                seller_id = safe_int(current_seller.get('id') or item.get('seller_id'))
                seller_name = safe_str(current_seller.get('name') or item.get('seller_name'))
                is_official = bool(current_seller.get('is_best_store') or False)

                # Collect seller info
                if seller_id and seller_id not in seen_sellers:
                    parsed_sellers.append({
                        "seller_id": seller_id,
                        "platform_seller_id": str(seller_id),
                        "seller_name": seller_name or f"Shop_{seller_id}",
                        "is_official_store": is_official
                    })
                    seen_sellers.add(seller_id)

                # Quantity sold parsing
                qty_sold_struct = item.get('quantity_sold') or {}
                qty_sold = 0
                if isinstance(qty_sold_struct, dict):
                    qty_sold = safe_int(qty_sold_struct.get('value'))
                else:
                    qty_sold = safe_int(qty_sold_struct)

                parsed_products.append({
                    "product_id": p_id,
                    "platform_product_id": str(p_id),
                    "product_name": safe_str(item.get('name'), "Sản phẩm Tiki"),
                    "category_name": cat_name,
                    "brand_name": brand_name,
                    "price": safe_float(item.get('price')),
                    "original_price": safe_float(item.get('original_price') or item.get('price')),
                    "discount_rate": safe_float(item.get('discount_rate') or item.get('discount')),
                    "quantity_sold": qty_sold,
                    "rating_average": safe_float(item.get('rating_average')),
                    "reviews_count": safe_int(item.get('reviews_count') or item.get('review_count')),
                    "seller_id": seller_id,
                    "event_date": date_str
                })

            # Write/Merge products to Delta Lake
            if parsed_products:
                df_prod = pd.DataFrame(parsed_products)
                df_prod = df_prod.drop_duplicates(subset=['platform_product_id'], keep='last')
                arrow_prod = pa.Table.from_pandas(df_prod, schema=PRODUCTS_SCHEMA, preserve_index=False)
                
                silver_prod_path = f"{silver_base.rstrip('/')}/products"
                s3_prod_path = silver_prod_path.replace("s3a://", "s3://")
                
                is_initialized = True
                try:
                    dt_prod = DeltaTable(s3_prod_path, storage_options=storage_options)
                except (TableNotFoundError, Exception) as init_err:
                    init_err_str = str(init_err)
                    if isinstance(init_err, TableNotFoundError) or "No files in log segment" in init_err_str or "not found" in init_err_str.lower():
                        is_initialized = False
                    else:
                        raise init_err

                if not is_initialized:
                    print(f"  - Initializing products Delta table at {s3_prod_path}...")
                    write_deltalake(
                        s3_prod_path,
                        arrow_prod,
                        mode="append",
                        storage_options=storage_options
                    )
                else:
                    print(f"  - Merging products into {s3_prod_path}...")
                    dt_prod.merge(
                        source=arrow_prod,
                        predicate="target.platform_product_id = source.platform_product_id",
                        source_alias="source",
                        target_alias="target"
                    ).when_matched_update_all().when_not_matched_insert_all().execute()
                
                if not skip_hive_sync:
                    sync_hive_delta_table(hive_db, "products", silver_prod_path, storage_options=storage_options)
            
            # Write/Merge sellers to Delta Lake
            if parsed_sellers:
                df_sel = pd.DataFrame(parsed_sellers)
                df_sel = df_sel.drop_duplicates(subset=['platform_seller_id'], keep='last')
                arrow_sel = pa.Table.from_pandas(df_sel, schema=SELLERS_SCHEMA, preserve_index=False)
                
                silver_sel_path = f"{silver_base.rstrip('/')}/sellers"
                s3_sel_path = silver_sel_path.replace("s3a://", "s3://")
                
                is_sel_initialized = True
                try:
                    dt_sel = DeltaTable(s3_sel_path, storage_options=storage_options)
                except (TableNotFoundError, Exception) as init_err:
                    init_err_str = str(init_err)
                    if isinstance(init_err, TableNotFoundError) or "No files in log segment" in init_err_str or "not found" in init_err_str.lower():
                        is_sel_initialized = False
                    else:
                        raise init_err

                if not is_sel_initialized:
                    print(f"  - Initializing sellers Delta table at {s3_sel_path}...")
                    write_deltalake(
                        s3_sel_path,
                        arrow_sel,
                        mode="append",
                        storage_options=storage_options
                    )
                else:
                    print(f"  - Merging sellers into {s3_sel_path}...")
                    dt_sel.merge(
                        source=arrow_sel,
                        predicate="target.platform_seller_id = source.platform_seller_id",
                        source_alias="source",
                        target_alias="target"
                    ).when_matched_update_all().when_not_matched_insert_all().execute()
                
                if not skip_hive_sync:
                    sync_hive_delta_table(hive_db, "sellers", silver_sel_path, storage_options=storage_options)

    except Exception as e:
        print(f"❌ Error processing products/sellers: {e}")

    # ==========================================
    # 2. PROCESS REVIEWS
    # ==========================================
    rev_prefix = f"provider=tiki/date={date_str}/category=reviews/"
    print(f"\n🔍 Searching for raw reviews with prefix: {rev_prefix}")
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bronze_bucket, Prefix=rev_prefix)
        
        raw_reviews_list = []
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('.json'):
                    continue
                
                # Extract product_id from key name (reviews_sp_<product_id>_<ts>.json)
                filename = key.split('/')[-1]
                match = re.search(r"reviews_sp_(\d+)_", filename)
                p_id = safe_int(match.group(1)) if match else 0
                
                try:
                    response = s3_client.get_object(Bucket=bronze_bucket, Key=key)
                    file_content = json.loads(response['Body'].read().decode('utf-8'))
                    
                    reviews_data = []
                    if isinstance(file_content, list):
                        reviews_data = file_content
                    elif isinstance(file_content, dict) and 'data' in file_content:
                        reviews_data = file_content['data']
                        
                    for rev in reviews_data:
                        rev_id = safe_int(rev.get('id'))
                        if not rev_id:
                            continue
                        
                        raw_reviews_list.append({
                            "review_id": rev_id,
                            "platform_review_id": str(rev_id),
                            "product_id": p_id,
                            "rating": safe_int(rev.get('rating')),
                            "content": safe_str(rev.get('content') or rev.get('review')),
                            "helpful_count": safe_int(rev.get('thank_count') or rev.get('helpful_count')),
                            "reviewed_at": safe_datetime(rev.get('created_at')),
                            "event_date": date_str
                        })
                except Exception as e:
                    print(f"  ⚠️ Error reading reviews file {key}: {e}")
                    
        print(f"  - Found {len(raw_reviews_list)} raw reviews in Bronze.")
        
        if raw_reviews_list:
            df_rev = pd.DataFrame(raw_reviews_list)
            
            # Fill missing reviewed_at with parsed date_str timestamp
            fallback_ts = pd.to_datetime(date_str)
            df_rev['reviewed_at'] = df_rev['reviewed_at'].fillna(fallback_ts)
            
            # Floor to microsecond resolution to prevent PyArrow conversion errors (datetime64[ns] to timestamp[us])
            df_rev['reviewed_at'] = pd.to_datetime(df_rev['reviewed_at']).dt.floor('us')
            
            df_rev = df_rev.drop_duplicates(subset=['platform_review_id'], keep='last')
            arrow_rev = pa.Table.from_pandas(df_rev, schema=REVIEWS_SCHEMA, preserve_index=False)
            
            silver_rev_path = f"{silver_base.rstrip('/')}/product_reviews"
            s3_rev_path = silver_rev_path.replace("s3a://", "s3://")
            
            is_rev_initialized = True
            try:
                dt_rev = DeltaTable(s3_rev_path, storage_options=storage_options)
            except (TableNotFoundError, Exception) as init_err:
                init_err_str = str(init_err)
                if isinstance(init_err, TableNotFoundError) or "No files in log segment" in init_err_str or "not found" in init_err_str.lower():
                    is_rev_initialized = False
                else:
                    raise init_err

            if not is_rev_initialized:
                print(f"  - Initializing product reviews Delta table at {s3_rev_path}...")
                write_deltalake(
                    s3_rev_path,
                    arrow_rev,
                    mode="append",
                    storage_options=storage_options
                )
            else:
                try:
                    print(f"  - Merging product reviews into {s3_rev_path}...")
                    dt_rev.merge(
                        source=arrow_rev,
                        predicate="target.platform_review_id = source.platform_review_id",
                        source_alias="source",
                        target_alias="target"
                    ).when_matched_update_all().when_not_matched_insert_all().execute()
                except Exception as merge_err:
                    print(f"  ❌ Merge failed: {merge_err}")
                    print(f"  - Source shape: {df_rev.shape}")
                    print(f"  - Source unique platform_review_id count: {df_rev['platform_review_id'].nunique()}")
                    if df_rev.duplicated(subset=['platform_review_id']).any():
                        print("  - Source duplicate platform_review_id entries:")
                        print(df_rev[df_rev.duplicated(subset=['platform_review_id'], keep=False)][['platform_review_id', 'product_id', 'rating']].head(10))
                    raise merge_err
                
            if not skip_hive_sync:
                sync_hive_delta_table(hive_db, "product_reviews", silver_rev_path, storage_options=storage_options)
                
    except Exception as e:
        print(f"❌ Error processing reviews: {e}")

def main():
    args = parse_args()
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    
    # Simple validation of YYYY-MM-DD
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        print(f"❌ Error: Invalid date format: '{date_str}'. Must be YYYY-MM-DD.")
        sys.exit(1)
        
    process_tiki_batch(
        date_str=date_str,
        bronze_bucket=args.bronze_bucket,
        silver_base=args.silver_base,
        hive_db=args.hive_db,
        skip_hive_sync=args.skip_hive_sync,
        clean=args.clean
    )
    print("\n🎉 Batch processing completed successfully!")

if __name__ == "__main__":
    main()
