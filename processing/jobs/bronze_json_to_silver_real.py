import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pandas_hive_utils import sync_hive_delta_table

load_dotenv()


PLATFORMS = {
    "tiki": {
        "platform_id": 1,
        "platform_code": "tiki",
        "platform_name": "Tiki",
        "base_url": "https://tiki.vn",
    },
    "sendo": {
        "platform_id": 2,
        "platform_code": "sendo",
        "platform_name": "Sendo",
        "base_url": "https://www.sendo.vn",
    },
    "chotot": {
        "platform_id": 3,
        "platform_code": "chotot",
        "platform_name": "Cho Tot",
        "base_url": "https://www.chotot.com",
    },
}

DEFAULT_TIKI_CATEGORIES = {
    1846: "Laptop - May Vi Tinh - Linh kien",
    1520: "Dien Thoai - May Tinh Bang",
    1789: "Do Choi - Me & Be",
    2549: "Do Gia Dung",
    8322: "Nha Cua - Doi Song",
    915: "Thiet Bi So - Phu Kien So",
}

PLATFORMS_SCHEMA = pa.schema(
    [
        ("platform_id", pa.int32()),
        ("platform_code", pa.string()),
        ("platform_name", pa.string()),
        ("base_url", pa.string()),
        ("is_active", pa.bool_()),
        ("created_at", pa.timestamp("us")),
        ("updated_at", pa.timestamp("us")),
    ]
)

PRODUCTS_SCHEMA = pa.schema(
    [
        ("product_id", pa.int64()),
        ("platform_id", pa.int32()),
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
        ("event_date", pa.string()),
    ]
)

SELLERS_SCHEMA = pa.schema(
    [
        ("seller_id", pa.int64()),
        ("platform_id", pa.int32()),
        ("platform_seller_id", pa.string()),
        ("seller_name", pa.string()),
        ("city", pa.string()),
        ("province", pa.string()),
        ("follower_count", pa.int64()),
        ("is_official_store", pa.bool_()),
        ("status", pa.string()),
        ("event_date", pa.string()),
    ]
)

REVIEWS_SCHEMA = pa.schema(
    [
        ("review_id", pa.int64()),
        ("platform_id", pa.int32()),
        ("platform_review_id", pa.string()),
        ("product_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("rating", pa.int64()),
        ("content", pa.string()),
        ("helpful_count", pa.int64()),
        ("reviewed_at", pa.timestamp("us")),
        ("event_date", pa.string()),
    ]
)

CUSTOMERS_SCHEMA = pa.schema(
    [
        ("customer_id", pa.int64()),
        ("platform_id", pa.int32()),
        ("platform_customer_id", pa.string()),
        ("full_name", pa.string()),
        ("avatar_url", pa.string()),
        ("created_time", pa.timestamp("us")),
        ("joined_time_summary", pa.string()),
        ("event_date", pa.string()),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process raw ecommerce Bronze JSON files to real Silver Delta tables."
    )
    parser.add_argument("--date", default=None, help="Date to process in YYYY-MM-DD. Defaults to today.")
    parser.add_argument(
        "--providers",
        default="tiki,sendo,chotot",
        help="Comma-separated providers to process. Default: tiki,sendo,chotot.",
    )
    parser.add_argument(
        "--bronze-bucket",
        default=os.getenv("MINIO_BUCKET_NAME", "bronze-lakehouse"),
        help="Bronze MinIO bucket name.",
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("SILVER_BASE_PATH", "s3a://silver-lakehouse/real_data"),
        help="Silver base path on MinIO.",
    )
    parser.add_argument("--hive-db", default="silver_real", help="Hive database name.")
    parser.add_argument(
        "--skip-hive-sync",
        action="store_true",
        default=os.getenv("SILVER_SKIP_HIVE_SYNC", "false").lower() == "true",
        help="Skip syncing Delta metadata to Hive Metastore.",
    )
    return parser.parse_args()


def resolve_endpoint(endpoint_url):
    import socket

    parsed = urlparse(endpoint_url)
    if parsed.hostname == "minio":
        try:
            socket.gethostbyname("minio")
        except socket.gaierror:
            endpoint_url = parsed._replace(netloc=parsed.netloc.replace("minio", "localhost")).geturl()
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
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password123"),
    )


def safe_int(value, default=0):
    try:
        if value is None or pd.isna(value):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_float(value, default=0.0):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_str(value, default=""):
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def safe_datetime(value, fallback=None):
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass

    try:
        text = str(value).strip()
        if not text:
            return fallback
        if text.replace(".", "", 1).isdigit() or (text.startswith("-") and text[1:].replace(".", "", 1).isdigit()):
            number = float(text)
            if abs(number) >= 1e15:
                return pd.to_datetime(number, unit="us")
            if abs(number) >= 1e12:
                return pd.to_datetime(number, unit="ms")
            return pd.to_datetime(number, unit="s")
        return pd.to_datetime(text)
    except Exception:
        return fallback


def stable_int_id(*parts):
    raw = ":".join(safe_str(part) for part in parts)
    if not raw.strip(":"):
        return 0
    digest = hashlib.blake2b(raw.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & ((1 << 63) - 1)


def load_category_map():
    category_map = DEFAULT_TIKI_CATEGORIES.copy()
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    for path in ("/app/crawler_state.txt", os.path.join(root, "crawler_state.txt"), os.path.join(os.getcwd(), "crawler_state.txt")):
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                state = json.load(handle)
            for category_id, info in state.items():
                if isinstance(info, dict) and info.get("category_name"):
                    category_map[safe_int(category_id)] = info["category_name"]
            print(f"Loaded category mappings from {path}")
            break
        except Exception as exc:
            print(f"Warning: could not load category map from {path}: {exc}")
    return category_map


def list_json_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                yield key


def read_json_object(s3_client, bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def extract_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("data", "ads", "products", "items", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = extract_records(value)
            if nested:
                return nested
    return []


def target_has_columns(delta_table, columns):
    target_cols = {field.name for field in delta_table.schema().fields}
    return all(column in target_cols for column in columns)


def dataframe_for_schema(records, schema):
    df = pd.DataFrame(records)
    for field in schema:
        if field.name not in df.columns:
            df[field.name] = None
    return df[[field.name for field in schema]]


def upsert_delta(records, schema, target_path, storage_options, merge_keys, table_name):
    if not records:
        return 0

    df = dataframe_for_schema(records, schema)
    if "reviewed_at" in df.columns:
        df["reviewed_at"] = pd.to_datetime(df["reviewed_at"], errors="coerce").dt.floor("us")
    if "created_time" in df.columns:
        df["created_time"] = pd.to_datetime(df["created_time"], errors="coerce").dt.floor("us")
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce").dt.floor("us")
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce").dt.floor("us")

    df = df.drop_duplicates(subset=merge_keys, keep="last")
    arrow_table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    s3_path = target_path.replace("s3a://", "s3://")

    try:
        delta_table = DeltaTable(s3_path, storage_options=storage_options)
    except (TableNotFoundError, Exception) as exc:
        exc_text = str(exc).lower()
        if isinstance(exc, TableNotFoundError) or "not found" in exc_text or "no files in log segment" in exc_text:
            print(f"  - Initializing {table_name}: {s3_path}")
            write_deltalake(s3_path, arrow_table, mode="append", storage_options=storage_options)
            return len(df)
        raise

    effective_keys = [key for key in merge_keys if target_has_columns(delta_table, [key])]
    if not effective_keys:
        print(f"  - Appending {table_name} with schema merge: {s3_path}")
        write_deltalake(s3_path, arrow_table, mode="append", schema_mode="merge", storage_options=storage_options)
        return len(df)

    predicate = " AND ".join(f"target.{key} = source.{key}" for key in effective_keys)
    print(f"  - Merging {len(df)} rows into {table_name}: {s3_path}")
    delta_table.merge(
        source=arrow_table,
        predicate=predicate,
        source_alias="source",
        target_alias="target",
        merge_schema=True,
    ).when_matched_update_all().when_not_matched_insert_all().execute()
    return len(df)


def platform_records(providers):
    now = pd.Timestamp.now("UTC").floor("us").tz_localize(None)
    records = []
    for provider in providers:
        config = PLATFORMS[provider]
        records.append(
            {
                **config,
                "is_active": True,
                "created_at": now,
                "updated_at": now,
            }
        )
    return records


def parse_tiki_product(item, date_str, category_map):
    platform_id = PLATFORMS["tiki"]["platform_id"]
    raw_id = safe_str(item.get("id"))
    product_id = safe_int(raw_id) or stable_int_id("tiki", raw_id)
    category_name = category_map.get(safe_int(item.get("category_id")), "Tiki Category")
    current_seller = item.get("current_seller") or {}
    seller_raw_id = safe_str(current_seller.get("id") or item.get("seller_id"))
    seller_id = safe_int(seller_raw_id) or stable_int_id("tiki", "seller", seller_raw_id)
    quantity_sold = item.get("quantity_sold") or {}
    if isinstance(quantity_sold, dict):
        quantity_sold = quantity_sold.get("value")
    brand = item.get("brand_name") or (item.get("brand") or {}).get("name")
    return (
        {
            "product_id": product_id,
            "platform_id": platform_id,
            "platform_product_id": raw_id,
            "product_name": safe_str(item.get("name"), "San pham Tiki"),
            "category_name": safe_str(category_name, "Tiki Category"),
            "brand_name": safe_str(brand, "Generic"),
            "price": safe_float(item.get("price")),
            "original_price": safe_float(item.get("original_price") or item.get("price")),
            "discount_rate": safe_float(item.get("discount_rate") or item.get("discount")),
            "quantity_sold": safe_int(quantity_sold),
            "rating_average": safe_float(item.get("rating_average")),
            "reviews_count": safe_int(item.get("reviews_count") or item.get("review_count")),
            "seller_id": seller_id,
            "event_date": date_str,
        },
        {
            "seller_id": seller_id,
            "platform_id": platform_id,
            "platform_seller_id": seller_raw_id or str(seller_id),
            "seller_name": safe_str(current_seller.get("name") or item.get("seller_name"), f"Shop_{seller_id}"),
            "city": None,
            "province": None,
            "follower_count": None,
            "is_official_store": bool(current_seller.get("is_best_store") or False),
            "status": "active",
            "event_date": date_str,
        },
    )


def parse_sendo_product(item, date_str):
    platform_id = PLATFORMS["sendo"]["platform_id"]
    raw_id = safe_str(item.get("product_id") or item.get("id"))
    product_id = stable_int_id("sendo", raw_id)
    region_id = safe_str(item.get("crawl_region_id") or item.get("region_id") or "default")
    seller_platform_id = f"sendo_farm_region_{region_id}"
    seller_id = stable_int_id("sendo", "seller", seller_platform_id)
    category_name = safe_str(item.get("tag_name") or item.get("category_name") or item.get("product_cate"), "Sendo Category")
    return (
        {
            "product_id": product_id,
            "platform_id": platform_id,
            "platform_product_id": raw_id,
            "product_name": safe_str(item.get("product_name") or item.get("name"), "San pham Sendo"),
            "category_name": category_name,
            "brand_name": safe_str(item.get("brand_name"), "Generic"),
            "price": safe_float(item.get("final_price") or item.get("price")),
            "original_price": safe_float(item.get("price") or item.get("final_price")),
            "discount_rate": safe_float(item.get("promotion_percent") or item.get("discount_rate")),
            "quantity_sold": safe_int(item.get("order_count")),
            "rating_average": safe_float(item.get("rating_average")),
            "reviews_count": safe_int(item.get("review_count") or item.get("total_rating")),
            "seller_id": seller_id,
            "event_date": date_str,
        },
        {
            "seller_id": seller_id,
            "platform_id": platform_id,
            "platform_seller_id": seller_platform_id,
            "seller_name": f"Sendo Farm Region {region_id}",
            "city": None,
            "province": None,
            "follower_count": None,
            "is_official_store": True,
            "status": "active",
            "event_date": date_str,
        },
    )


def parse_chotot_product(item, date_str):
    platform_id = PLATFORMS["chotot"]["platform_id"]
    raw_id = safe_str(item.get("ad_id") or item.get("list_id") or item.get("id"))
    product_id = stable_int_id("chotot", raw_id)
    seller_raw_id = safe_str(item.get("account_id") or item.get("account_oid") or "unknown")
    seller_id = stable_int_id("chotot", "seller", seller_raw_id)
    seller_info = item.get("seller_info") or {}
    official_value = item.get("official_store")
    return (
        {
            "product_id": product_id,
            "platform_id": platform_id,
            "platform_product_id": raw_id,
            "product_name": safe_str(item.get("subject") or item.get("title"), "Tin Cho Tot"),
            "category_name": safe_str(item.get("category_name"), "Cho Tot Category"),
            "brand_name": "Generic",
            "price": safe_float(item.get("price")),
            "original_price": safe_float(item.get("price")),
            "discount_rate": 0.0,
            "quantity_sold": safe_int(item.get("sold_ads")),
            "rating_average": safe_float(item.get("average_rating") or item.get("average_rating_for_seller")),
            "reviews_count": safe_int(item.get("total_rating") or item.get("total_rating_for_seller")),
            "seller_id": seller_id,
            "event_date": date_str,
        },
        {
            "seller_id": seller_id,
            "platform_id": platform_id,
            "platform_seller_id": seller_raw_id,
            "seller_name": safe_str(item.get("account_name") or item.get("full_name") or seller_info.get("full_name"), f"Seller_{seller_raw_id}"),
            "city": safe_str(item.get("area_name") or item.get("ward_name"), None),
            "province": safe_str(item.get("region_name") or item.get("region_name_v3"), None),
            "follower_count": safe_int(item.get("sold_ads") or seller_info.get("sold_ads")),
            "is_official_store": str(official_value).lower() in ("1", "true", "yes"),
            "status": safe_str(item.get("status") or item.get("state"), "active"),
            "event_date": date_str,
        },
    )


def parse_product(provider, item, date_str, category_map):
    if provider == "tiki":
        return parse_tiki_product(item, date_str, category_map)
    if provider == "sendo":
        return parse_sendo_product(item, date_str)
    if provider == "chotot":
        return parse_chotot_product(item, date_str)
    raise ValueError(f"Unsupported provider: {provider}")


def parse_review(provider, item, product_id, date_str):
    platform_id = PLATFORMS[provider]["platform_id"]
    raw_review_id = safe_str(item.get("id") or item.get("review_id"))
    if not raw_review_id:
        return None, None

    review_id = safe_int(raw_review_id) if provider == "tiki" else stable_int_id(provider, "review", raw_review_id)
    customer_payload = item.get("created_by") or item.get("customer") or {}
    raw_customer_id = safe_str(customer_payload.get("id") or item.get("customer_id"))
    customer_id = safe_int(raw_customer_id) if provider == "tiki" else stable_int_id(provider, "customer", raw_customer_id)
    reviewed_at = safe_datetime(item.get("created_at") or item.get("reviewed_at"), pd.to_datetime(date_str))
    review = {
        "review_id": review_id,
        "platform_id": platform_id,
        "platform_review_id": raw_review_id,
        "product_id": product_id,
        "customer_id": customer_id,
        "rating": safe_int(item.get("rating")),
        "content": safe_str(item.get("content") or item.get("review")),
        "helpful_count": safe_int(item.get("thank_count") or item.get("helpful_count")),
        "reviewed_at": reviewed_at,
        "event_date": date_str,
    }
    customer = None
    if raw_customer_id:
        contribute_info = customer_payload.get("contribute_info") or {}
        summary = contribute_info.get("summary") or {}
        customer = {
            "customer_id": customer_id,
            "platform_id": platform_id,
            "platform_customer_id": raw_customer_id,
            "full_name": safe_str(customer_payload.get("full_name") or customer_payload.get("name"), f"User_{customer_id}"),
            "avatar_url": safe_str(customer_payload.get("avatar_url") or customer_payload.get("avatar")),
            "created_time": safe_datetime(customer_payload.get("created_time")),
            "joined_time_summary": safe_str(summary.get("joined_time")),
            "event_date": date_str,
        }
    return review, customer


def load_provider_products(s3_client, bucket, provider, date_str, category_map):
    products = []
    sellers = []
    seen_products = set()
    seen_sellers = set()
    prefixes = [
        f"provider={provider}/date={date_str}/category=products/",
        f"provider={provider}/date={date_str}/category=listings/",
    ]

    for prefix in prefixes:
        keys = list(list_json_objects(s3_client, bucket, prefix))
        if not keys:
            continue
        print(f"  - Found {len(keys)} product/listing files under {prefix}")
        for key in keys:
            try:
                for item in extract_records(read_json_object(s3_client, bucket, key)):
                    product, seller = parse_product(provider, item, date_str, category_map)
                    if product["platform_product_id"] and product["platform_product_id"] not in seen_products:
                        products.append(product)
                        seen_products.add(product["platform_product_id"])
                    seller_key = seller["platform_seller_id"]
                    if seller_key and seller_key not in seen_sellers:
                        sellers.append(seller)
                        seen_sellers.add(seller_key)
            except Exception as exc:
                print(f"    Warning: could not process {key}: {exc}")

    return products, sellers


def load_provider_reviews(s3_client, bucket, provider, date_str):
    reviews = []
    customers = []
    seen_reviews = set()
    seen_customers = set()
    prefix = f"provider={provider}/date={date_str}/category=reviews/"
    keys = list(list_json_objects(s3_client, bucket, prefix))
    if not keys:
        print(f"  - No review files under {prefix}")
        return reviews, customers

    print(f"  - Found {len(keys)} review files under {prefix}")
    for key in keys:
        filename = key.rsplit("/", 1)[-1]
        product_match = re.search(r"reviews_sp_([^_]+)_", filename)
        raw_product_id = product_match.group(1) if product_match else None
        product_id = safe_int(raw_product_id) if provider == "tiki" else stable_int_id(provider, raw_product_id)
        try:
            for item in extract_records(read_json_object(s3_client, bucket, key)):
                review, customer = parse_review(provider, item, product_id, date_str)
                if review and review["platform_review_id"] not in seen_reviews:
                    reviews.append(review)
                    seen_reviews.add(review["platform_review_id"])
                if customer and customer["platform_customer_id"] not in seen_customers:
                    customers.append(customer)
                    seen_customers.add(customer["platform_customer_id"])
        except Exception as exc:
            print(f"    Warning: could not process {key}: {exc}")
    return reviews, customers


def process_batch(date_str, providers, bronze_bucket, silver_base, hive_db, skip_hive_sync):
    s3_client = get_s3_client()
    storage_options = get_storage_options()
    category_map = load_category_map()

    print(f"Processing Bronze JSON date={date_str}, providers={','.join(providers)}")
    print(f"Bronze bucket: {bronze_bucket}")
    print(f"Silver base: {silver_base}")

    written = {}
    table_specs = {
        "platforms": (PLATFORMS_SCHEMA, ["platform_id"]),
        "products": (PRODUCTS_SCHEMA, ["platform_id", "platform_product_id"]),
        "sellers": (SELLERS_SCHEMA, ["platform_id", "platform_seller_id"]),
        "product_reviews": (REVIEWS_SCHEMA, ["platform_id", "platform_review_id"]),
        "customers": (CUSTOMERS_SCHEMA, ["platform_id", "platform_customer_id"]),
    }

    platforms_count = upsert_delta(
        platform_records(providers),
        PLATFORMS_SCHEMA,
        f"{silver_base.rstrip('/')}/platforms",
        storage_options,
        ["platform_id"],
        "platforms",
    )
    written["platforms"] = platforms_count

    all_products = []
    all_sellers = []
    all_reviews = []
    all_customers = []

    for provider in providers:
        print(f"\nProvider: {provider}")
        products, sellers = load_provider_products(s3_client, bronze_bucket, provider, date_str, category_map)
        reviews, customers = load_provider_reviews(s3_client, bronze_bucket, provider, date_str)
        print(f"  - Parsed {len(products)} products, {len(sellers)} sellers, {len(reviews)} reviews, {len(customers)} customers")
        all_products.extend(products)
        all_sellers.extend(sellers)
        all_reviews.extend(reviews)
        all_customers.extend(customers)

    payloads = {
        "products": all_products,
        "sellers": all_sellers,
        "product_reviews": all_reviews,
        "customers": all_customers,
    }

    for table_name, records in payloads.items():
        schema, merge_keys = table_specs[table_name]
        count = upsert_delta(
            records,
            schema,
            f"{silver_base.rstrip('/')}/{table_name}",
            storage_options,
            merge_keys,
            table_name,
        )
        written[table_name] = count

    if not skip_hive_sync:
        for table_name, count in written.items():
            if count <= 0:
                continue
            try:
                sync_hive_delta_table(
                    hive_db,
                    table_name,
                    f"{silver_base.rstrip('/')}/{table_name}",
                    storage_options=storage_options,
                )
            except Exception as exc:
                print(f"Warning: Hive sync failed for {table_name}: {exc}")

    print("\nWritten rows:")
    for table_name, count in written.items():
        print(f"  - {table_name}: {count}")


def main():
    args = parse_args()
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        print(f"Invalid date format: {date_str}. Expected YYYY-MM-DD.")
        sys.exit(1)

    providers = [item.strip().lower() for item in args.providers.split(",") if item.strip()]
    unsupported = sorted(set(providers) - set(PLATFORMS))
    if unsupported:
        print(f"Unsupported providers: {', '.join(unsupported)}")
        sys.exit(1)

    process_batch(
        date_str=date_str,
        providers=providers,
        bronze_bucket=args.bronze_bucket,
        silver_base=args.silver_base,
        hive_db=args.hive_db,
        skip_hive_sync=args.skip_hive_sync,
    )


if __name__ == "__main__":
    main()
