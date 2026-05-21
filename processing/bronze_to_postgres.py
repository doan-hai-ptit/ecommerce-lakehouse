import argparse
import os
import re
import sys
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

load_dotenv()

SUPPORTED_SOURCES = ("tiki", "shopee", "sendo", "chotot")

PLATFORMS = {
    "tiki": ("Tiki", "https://tiki.vn"),
    "shopee": ("Shopee", "https://shopee.vn"),
    "sendo": ("Sendo", "https://sendo.vn"),
    "chotot": ("Cho Tot", "https://www.chotot.com"),
}

DEDUPE_KEYS = {
        "sellers": ["platform_code", "platform_seller_id"],
        "categories": ["platform_code", "platform_category_id"],
        "brands": ["platform_code", "brand_name"],
        "customers": ["platform_code", "platform_customer_id"],
        "products": ["platform_code", "platform_product_id"],
        "product_variants": ["platform_code", "platform_product_id", "platform_variant_id"],
        "product_inventory": ["platform_code", "platform_product_id", "platform_variant_id"],
        "product_reviews": ["platform_code", "platform_review_id"],
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load Bronze marketplace JSON from MinIO into PostgreSQL operational tables."
    )
    parser.add_argument(
        "source",
        choices=[*SUPPORTED_SOURCES, "all"],
        help="Nguồn dữ liệu cần xử lý: tiki, shopee, sendo, chotot hoặc all.",
    )
    parser.add_argument(
        "--date",
        default="*",
        help="Partition date cần đọc, ví dụ 2026-05-15. Mặc định đọc tất cả date=*.",
    )
    parser.add_argument(
        "--bronze-base",
        default=os.getenv("BRONZE_BASE_PATH", "s3a://bronze-lakehouse"),
        help="Bronze base path. Có thể là s3a://bronze-lakehouse hoặc ingestion/batch/raw_data.",
    )
    parser.add_argument(
        "--write-delta",
        action="store_true",
        help="Ghi thêm bản chuẩn hóa xuống MinIO/Delta để debug hoặc tái dùng.",
    )
    parser.add_argument(
        "--silver-base",
        default=os.getenv("POSTGRES_SEED_SILVER_PATH", "s3a://silver-lakehouse/"),
        help="Base path khi bật --write-delta.",
    )
    parser.add_argument(
        "--hive-db",
        default=os.getenv("SILVER_HIVE_DATABASE", "silver"),
        help="Hive database dùng để đăng ký metadata cho các Delta table.",
    )
    parser.add_argument(
        "--sync-hive",
        action="store_true",
        help="Đăng ký/đồng bộ metadata Delta vào Hive Metastore sau khi ghi.",
    )
    return parser.parse_args()


def get_env(*names, default=None):
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def build_spark(app_name, enable_hive_support=False):
    endpoint_url = get_env("MINIO_ENDPOINT_URL", default="http://minio:9000")
    access_key = get_env("MINIO_ACCESS_KEY", "AWS_ACCESS_KEY_ID", default="admin")
    secret_key = get_env("MINIO_SECRET_KEY", "AWS_SECRET_ACCESS_KEY", default="password123")
    warehouse_dir = get_env("SPARK_WAREHOUSE_DIR", default="file:/tmp/spark-warehouse")

    if warehouse_dir.startswith("file:"):
        os.makedirs(warehouse_dir.replace("file:", "", 1), exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # 1. Quản lý Warehouse local
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("hive.metastore.warehouse.dir", warehouse_dir)
        .config("spark.hadoop.hive.metastore.warehouse.dir", warehouse_dir)
        .config("spark.sql.hive.manageFilesourceTables", "false")
        
        # 2. Cấu hình S3A / MinIO
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.hive.metastore.authorization.storage.checks", "false")
       
        # 3. Tắt HOÀN TOÀN tính năng tự động tính toán Stats của Hive để chống kẹt DB
        .config("spark.sql.statistics.fallBackToHdfs", "false")
        .config("hive.stats.autogather", "false")
        .config("hive.stats.column.autogather", "false")  # Bổ sung dòng này
        .config("hive.gather.stats.skip.empty", "true")   # Bổ sung dòng này

        # 4. Cấu hình Delta
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.jsonGenerator.ignoreNullFields", "false")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    )
    if enable_hive_support:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()

def bronze_path(base_path, source, date_part, category):
    return f"{base_path.rstrip('/')}/provider={source}/date={date_part}/category={category}/*.json"


def read_json(spark, path):
    try:
        df = spark.read.option("multiline", "true").json(path)
        if not df.columns:
            return None
        return df.withColumn("_source_file", F.input_file_name())
    except Exception as exc:
        print(f"⚠ Bỏ qua path không đọc được hoặc chưa có dữ liệu: {path} ({exc})")
        return None


def has_field(schema, path):
    current = schema
    for part in path.split("."):
        if isinstance(current, StructType):
            match = next((field for field in current.fields if field.name == part), None)
            if match is None:
                return False
            current = match.dataType
        else:
            return False
    return True


def field(df, path, data_type=None):
    if not has_field(df.schema, path):
        return F.lit(None).cast(data_type) if data_type else F.lit(None)
    
    # Duyệt qua schema để lấy kiểu dữ liệu thực tế của trường
    current = df.schema
    for part in path.split("."):
        if isinstance(current, StructType):
            current = next((f.dataType for f in current.fields if f.name == part), None)
        else:
            current = None
            break
            
    # NẾU kiểu dữ liệu thực tế là phức tạp (Array hoặc Struct) 
    # MÀ code lại yêu cầu ép kiểu số/chữ cơ bản -> Trả về NULL để tránh sập job
    if data_type and isinstance(current, (ArrayType, StructType)):
        return F.lit(None).cast(data_type)
        
    col = F.col(path)
    return col.cast(data_type) if data_type else col


def array_first(df, path, data_type="string"):
    if not has_field(df.schema, path):
        return F.lit(None).cast(data_type)

    col = F.col(path)
    field_type = df.schema
    for part in path.split("."):
        field_type = next(item for item in field_type.fields if item.name == part).dataType

    if isinstance(field_type, ArrayType):
        return F.element_at(col, 1).cast(data_type)

    return col.cast(data_type)


def clean_string(col):
    value = F.trim(col.cast("string"))
    return F.when(F.length(value) > 0, value)


def coalesce_string(*cols):
    return F.coalesce(*[clean_string(col) for col in cols])


def clamp_numeric(col, lower=None, upper=None):
    value = col.cast("double")
    if lower is not None:
        value = F.when(value < F.lit(lower), F.lit(lower)).otherwise(value)
    if upper is not None:
        value = F.when(value > F.lit(upper), F.lit(upper)).otherwise(value)
    return value


def from_unix_or_text(col):
    as_long = col.cast("long")
    return F.when(
        as_long.isNotNull(),
        F.to_timestamp(F.from_unixtime(as_long)),
    ).otherwise(F.to_timestamp(col.cast("string")))


def first_not_null_by_key(df, keys):
    if df is None:
        return None

    order_col = F.monotonically_increasing_id()
    window = Window.partitionBy(*keys).orderBy(order_col)
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def normalize_status(raw_status, availability=None, stock=None):
    raw = F.lower(coalesce_string(raw_status, F.lit("active")))
    status = (
        F.when(raw.isin("deleted", "delete", "removed"), F.lit("deleted"))
        .when(raw.isin("inactive", "disabled"), F.lit("inactive"))
        .when(raw.contains("out") | raw.contains("sold"), F.lit("out_of_stock"))
        .otherwise(F.lit("active"))
    )
    if availability is not None:
        status = F.when(availability.cast("int") == 0, F.lit("out_of_stock")).otherwise(status)
    if stock is not None:
        status = F.when(stock.cast("long") == 0, F.lit("out_of_stock")).otherwise(status)
    return status


def product_url(source, df):
    url = coalesce_string(
        field(df, "product_url"),
        field(df, "url"),
        field(df, "url_key"),
        field(df, "url_path"),
        field(df, "list_id"),
    )
    if source == "tiki":
        return F.when(url.startswith("http"), url).otherwise(F.concat(F.lit("https://tiki.vn/"), url))
    if source == "shopee":
        return url
    if source == "sendo":
        return F.when(url.startswith("http"), url).otherwise(F.concat(F.lit("https://www.sendo.vn/"), url))
    return F.when(url.startswith("http"), url).otherwise(F.concat(F.lit("https://www.chotot.com/"), url))


def image_urls_json(source, df):
    thumbnail = coalesce_string(
        field(df, "thumbnail_url"),
        field(df, "image_url"),
        field(df, "image"),
        field(df, "thumbnail"),
    )
    if has_field(df.schema, "images"):
        return F.to_json(F.col("images"))
    return F.when(thumbnail.isNotNull(), F.to_json(F.array(thumbnail)))


def source_product_id(source, df):
    if source == "chotot":
        return coalesce_string(field(df, "ad_id"), field(df, "list_id"), field(df, "id"))
    return coalesce_string(field(df, "id"), field(df, "product_id"), field(df, "itemid"))


def source_seller_id(source, df):
    if source == "chotot":
        return coalesce_string(
            field(df, "account_id"),
            field(df, "account_oid"),
            field(df, "account.id"),
            field(df, "seller_id"),
        )
    return coalesce_string(field(df, "seller_id"), field(df, "shopid"), field(df, "shop_id"))


def source_seller_name(df):
    return coalesce_string(
        field(df, "seller_name"),
        field(df, "shop_name"),
        field(df, "account_name"),
        field(df, "account.name"),
        field(df, "username"),
        F.lit("Unknown Seller"),
    )


def source_category_id(df):
    return coalesce_string(
        field(df, "category_id"),
        field(df, "catid"),
        field(df, "metadata.catid"),
        field(df, "crawl_category_id"),
        array_first(df, "category_ids"),
        field(df, "category"),
        field(df, "category_name"),
    )


def source_category_name(df):
    category_id = source_category_id(df)
    return coalesce_string(
        field(df, "primary_category_name"),
        field(df, "category_name"),
        field(df, "cat_name"),
        field(df, "category"),
        field(df, "tag_name"),
        F.concat(F.lit("category_"), category_id),
    )


def source_brand_id(df):
    return coalesce_string(field(df, "brand_id"), field(df, "brand.id"))


def source_brand_name(df):
    return coalesce_string(field(df, "brand_name"), field(df, "brand"), field(df, "tag_name"), F.lit("No Brand"))


def source_stock(df):
    return F.coalesce(
        field(df, "stock", "long"),
        field(df, "quantity", "long"),
        field(df, "stock_item.qty", "long"),
        F.lit(0).cast("long"),
    )


def source_sold_count(df):
    return F.coalesce(
        field(df, "quantity_sold.value", "long"),
        field(df, "sold", "long"),
        field(df, "historical_sold", "long"),
        field(df, "all_time_quantity_sold", "long"),
        field(df, "order_count", "long"),
        F.lit(0).cast("long"),
    )


def source_review_count(df):
    return F.coalesce(field(df, "review_count", "long"), field(df, "cmt_count", "long"), F.lit(0).cast("long"))

def normalize_products(source, products_df):
    if products_df is None:
        return {}

    # Trích xuất ngày từ đường dẫn tệp để làm cột phân vùng
    part_date = F.regexp_extract(F.col("_source_file"), r"date=([^/]+)", 1).alias("partition_date")

    platform_product_id = source_product_id(source, products_df)
    seller_id = F.coalesce(source_seller_id(source, products_df), F.lit("__unknown__"))
    seller_name = source_seller_name(products_df)
    category_id = source_category_id(products_df)
    category_name = source_category_name(products_df)
    brand_id = source_brand_id(products_df)
    brand_name = source_brand_name(products_df)
    stock = source_stock(products_df)
    original_price = F.coalesce(
        field(products_df, "original_price", "double"),
        field(products_df, "price_before_discount", "double"),
        field(products_df, "price", "double"),
        field(products_df, "final_price", "double"),
        F.lit(0.0),
    )
    sale_price = F.coalesce(
        field(products_df, "final_price", "double"),
        field(products_df, "price", "double"),
        field(products_df, "price_min", "double"),
        original_price,
    )

    sellers = products_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        seller_id.alias("platform_seller_id"),
        seller_name.alias("seller_name"),
        coalesce_string(field(products_df, "shop_url"), field(products_df, "url")).alias("shop_url"),
        coalesce_string(field(products_df, "seller_logo"), field(products_df, "logo_url")).alias("logo_url"),
        coalesce_string(field(products_df, "shop_location"), field(products_df, "city")).alias("city"),
        coalesce_string(field(products_df, "province"), field(products_df, "region_name")).alias("province"),
        clamp_numeric(
            F.coalesce(field(products_df, "seller_rating", "double"), field(products_df, "shop_rating", "double")),
            0,
            5,
        ).alias("rating_average"),
        field(products_df, "follower_count", "int").alias("follower_count"),
        F.coalesce(field(products_df, "is_from_official_store", "boolean"), field(products_df, "metadata.is_official_shop", "boolean"), F.lit(False)).alias("is_official_store"),
    ).where(F.col("platform_seller_id").isNotNull())

    categories = products_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        category_id.alias("platform_category_id"),
        category_name.alias("category_name"),
        coalesce_string(field(products_df, "primary_category_path"), field(products_df, "category_path")).alias("category_path"),
        F.lit(0).cast("int").alias("level"),
    ).where(F.col("platform_category_id").isNotNull() & F.col("category_name").isNotNull())

    brands = products_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        brand_id.alias("platform_brand_id"),
        brand_name.alias("brand_name"),
        F.lit(None).cast("string").alias("country"),
        F.coalesce(field(products_df, "is_top_brand", "boolean"), F.lit(False)).alias("is_official"),
    ).where(F.col("brand_name").isNotNull())

    products = products_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        platform_product_id.alias("platform_product_id"),
        seller_id.alias("platform_seller_id"),
        category_id.alias("platform_category_id"),
        brand_id.alias("platform_brand_id"),
        brand_name.alias("brand_name"),
        coalesce_string(field(products_df, "name"), field(products_df, "product_name"), field(products_df, "subject")).alias("product_name"),
        coalesce_string(field(products_df, "description"), field(products_df, "body")).alias("description"),
        product_url(source, products_df).alias("product_url"),
        coalesce_string(field(products_df, "thumbnail_url"), field(products_df, "image_url"), field(products_df, "image")).alias("thumbnail_url"),
        image_urls_json(source, products_df).alias("image_urls"),
        normalize_status(field(products_df, "status"), field(products_df, "availability"), stock).alias("status"),
        clamp_numeric(
            F.coalesce(field(products_df, "rating_average", "double"), field(products_df, "product_rating", "double")),
            0,
            5,
        ).alias("rating_average"),
        source_review_count(products_df).cast("int").alias("review_count"),
        source_sold_count(products_df).cast("int").alias("sold_count"),
        F.coalesce(field(products_df, "view_count", "int"), field(products_df, "metadata.view_count", "int"), F.lit(0)).alias("view_count"),
        F.coalesce(field(products_df, "liked_count", "int"), field(products_df, "metadata.liked_count", "int"), F.lit(0)).alias("liked_count"),
        F.coalesce(field(products_df, "is_authentic", "boolean"), field(products_df, "tiki_verified", "boolean")).alias("is_authentic"),
        F.coalesce(field(products_df, "is_from_official_store", "boolean"), field(products_df, "metadata.is_official_shop", "boolean")).alias("is_official_store"),
        from_unix_or_text(F.coalesce(field(products_df, "date"), field(products_df, "published_at"))).alias("published_at"),
    ).where(F.col("platform_product_id").isNotNull() & F.col("product_name").isNotNull())

    variants = products_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        platform_product_id.alias("platform_product_id"),
        coalesce_string(
            field(products_df, "seller_product_id"),
            field(products_df, "spid"),
            field(products_df, "sku"),
            field(products_df, "product_sku"),
            platform_product_id,
        ).alias("platform_variant_id"),
        coalesce_string(field(products_df, "sku"), field(products_df, "seller_product_sku"), field(products_df, "product_sku")).alias("sku"),
        coalesce_string(field(products_df, "name"), field(products_df, "product_name"), field(products_df, "subject")).alias("variant_name"),
        F.lit("{}").cast("string").alias("option_values"),
        F.lit("VND").alias("currency"),
        original_price.cast("double").alias("original_price"),
        sale_price.cast("double").alias("sale_price"),
        F.coalesce(
            field(products_df, "discount_rate", "double"),
            F.when(original_price > 0, ((original_price - sale_price) / original_price) * 100),
        ).cast("double").alias("discount_rate"),
        field(products_df, "barcode").cast("string").alias("barcode"),
        field(products_df, "weight").cast("int").alias("weight_gram"),
        normalize_status(field(products_df, "status"), field(products_df, "availability"), stock).alias("status"),
    ).where(F.col("platform_product_id").isNotNull())

    inventory = products_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        platform_product_id.alias("platform_product_id"),
        coalesce_string(
            field(products_df, "seller_product_id"),
            field(products_df, "spid"),
            field(products_df, "sku"),
            field(products_df, "product_sku"),
            platform_product_id,
        ).alias("platform_variant_id"),
        F.lit("default").alias("warehouse_code"),
        F.when(stock < 0, F.lit(0)).otherwise(stock).cast("int").alias("quantity_on_hand"),
        F.lit(0).cast("int").alias("quantity_reserved"),
        F.lit(0).cast("int").alias("low_stock_threshold"),
    ).where(F.col("platform_product_id").isNotNull())

    return {
        "sellers": sellers,
        "categories": categories,
        "brands": brands,
        "products": products,
        "product_variants": variants,
        "product_inventory": inventory,
    }


def normalize_seller_files(source, sellers_df):
    if sellers_df is None:
        return None

    part_date = F.regexp_extract(F.col("_source_file"), r"date=([^/]+)", 1).alias("partition_date")
    platform_seller_id = F.coalesce(source_seller_id(source, sellers_df), coalesce_string(field(sellers_df, "id")))
    
    return sellers_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        platform_seller_id.alias("platform_seller_id"),
        coalesce_string(source_seller_name(sellers_df), F.concat(F.lit("seller_"), platform_seller_id)).alias("seller_name"),
        coalesce_string(field(sellers_df, "shop_url"), field(sellers_df, "url")).alias("shop_url"),
        coalesce_string(field(sellers_df, "seller_logo"), field(sellers_df, "logo_url")).alias("logo_url"),
        coalesce_string(field(sellers_df, "city")).alias("city"),
        coalesce_string(field(sellers_df, "province")).alias("province"),
        clamp_numeric(field(sellers_df, "seller_rating", "double"), 0, 5).alias("rating_average"),
        field(sellers_df, "follower_count", "int").alias("follower_count"),
        F.coalesce(field(sellers_df, "is_official_store", "boolean"), F.lit(False)).alias("is_official_store"),
    ).where(F.col("platform_seller_id").isNotNull())


def normalize_reviews(source, reviews_df):
    if reviews_df is None:
        return {}

    part_date = F.regexp_extract(F.col("_source_file"), r"date=([^/]+)", 1).alias("partition_date")
    file_product_id = F.regexp_extract(F.col("_source_file"), r"reviews_sp_([0-9]+)", 1)
    product_id = coalesce_string(field(reviews_df, "product_id"), field(reviews_df, "itemid"), file_product_id)
    customer_id = coalesce_string(
        field(reviews_df, "customer_id"),
        field(reviews_df, "userid"),
        field(reviews_df, "author_username"),
        field(reviews_df, "created_by.id"),
    )
    seller_id = coalesce_string(field(reviews_df, "seller.id"), field(reviews_df, "shopid"), field(reviews_df, "seller_id"))
    review_id = coalesce_string(
        field(reviews_df, "id"),
        field(reviews_df, "cmtid"),
        field(reviews_df, "comment_id"),
        F.sha2(F.to_json(F.struct(*[F.col(col) for col in reviews_df.columns if col != "_source_file"])), 256),
    )
    raw_status = F.lower(coalesce_string(field(reviews_df, "status"), F.lit("published")))
    status = (
        F.when(raw_status.isin("approved", "published", "normal"), F.lit("published"))
        .when(raw_status.isin("hidden", "hide"), F.lit("hidden"))
        .when(raw_status.isin("deleted", "removed"), F.lit("deleted"))
        .otherwise(F.lit("pending"))
    )

    customers = reviews_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        customer_id.alias("platform_customer_id"),
        coalesce_string(
            field(reviews_df, "created_by.full_name"),
            field(reviews_df, "created_by.name"),
            field(reviews_df, "author_username"),
        ).alias("full_name"),
        F.lit(None).cast("string").alias("email"),
        F.lit(None).cast("string").alias("phone_number"),
        F.lit("unknown").alias("gender"),
        F.lit(None).cast("date").alias("date_of_birth"),
        from_unix_or_text(field(reviews_df, "created_by.created_time")).alias("registered_at"),
    ).where(F.col("platform_customer_id").isNotNull())

    reviews = reviews_df.select(
        part_date,
        F.lit(source).alias("platform_code"),
        review_id.alias("platform_review_id"),
        product_id.alias("platform_product_id"),
        customer_id.alias("platform_customer_id"),
        seller_id.alias("platform_seller_id"),
        F.coalesce(field(reviews_df, "rating", "int"), field(reviews_df, "rating_star", "int")).alias("rating"),
        coalesce_string(field(reviews_df, "title")).alias("title"),
        coalesce_string(field(reviews_df, "content"), field(reviews_df, "comment")).alias("content"),
        review_media_urls(reviews_df).alias("media_urls"),
        field(reviews_df, "delivery_rating", "int").alias("delivery_rating"),
        field(reviews_df, "seller_rating", "int").alias("seller_rating"),
        F.coalesce(field(reviews_df, "thank_count", "int"), field(reviews_df, "like_count", "int"), F.lit(0)).alias("helpful_count"),
        status.alias("status"),
        from_unix_or_text(F.coalesce(field(reviews_df, "created_at"), field(reviews_df, "ctime"))).alias("reviewed_at"),
    ).where(
        F.col("platform_review_id").isNotNull()
        & F.col("platform_product_id").isNotNull()
        & F.col("rating").between(1, 5)
    )

    return {"customers": customers, "product_reviews": reviews}

def review_media_urls(df):
    if has_field(df.schema, "images"):
        return F.to_json(F.col("images"))
    return F.lit(None).cast("string")

def union_by_name(dfs):
    dfs = [df for df in dfs if df is not None]
    if not dfs:
        return None
    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)
    return result

def collect_normalized(spark, source, bronze_base, date_part):
    products_df = read_json(spark, bronze_path(bronze_base, source, date_part, "products"))
    reviews_df = read_json(spark, bronze_path(bronze_base, source, date_part, "reviews"))
    sellers_df = read_json(spark, bronze_path(bronze_base, source, date_part, "sellers"))

    normalized = normalize_products(source, products_df)
    seller_file_df = normalize_seller_files(source, sellers_df)
    if seller_file_df is not None:
        normalized["sellers"] = union_by_name([normalized.get("sellers"), seller_file_df])

    review_normalized = normalize_reviews(source, reviews_df)
    for table_name, df in review_normalized.items():
        normalized[table_name] = union_by_name([normalized.get(table_name), df])

    # Thay đổi ở đây: Trỏ trực tiếp tới cấu trúc DEDUPE_KEYS toàn cục
    return {
        table_name: first_not_null_by_key(df, DEDUPE_KEYS[table_name])
        for table_name, df in normalized.items()
        if df is not None and table_name in DEDUPE_KEYS
    }

def write_delta_silver(tables, silver_base, dedupe_keys, hive_db, sync_hive=False):
    """
    Ghi dữ liệu Silver layer xuống MinIO theo định dạng Delta Lake,
    phân vùng theo cột 'partition_date'. Metadata Hive chỉ được đồng bộ khi bật sync_hive.
    """
    from delta.tables import DeltaTable

    for table_name, df in tables.items():
        if df is None:
            continue
            
        target_path = f"{silver_base.rstrip('/')}/{table_name}"
        hive_table_name = qualified_table_name(hive_db, table_name)
        spark = df.sparkSession
        
        sync_target = f" | Table: {hive_table_name}" if sync_hive else ""
        print(f"💾 [Delta] Đang xử lý bảng {table_name} -> Path: {target_path}{sync_target}")

        # Kiểm tra xem Delta Table đã được khởi tạo tại bucket chưa
        if DeltaTable.isDeltaTable(spark, target_path):
            delta_table = DeltaTable.forPath(spark, target_path)
            
            # 1. Xây dựng điều kiện ON phối hợp khóa chính
            keys = dedupe_keys.get(table_name, ["platform_code"])
            merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])
            
            # TỐI ƯU HÓA: Ép kiểm tra thêm trường phân vùng ngày để kích hoạt Partition Pruning
            if "partition_date" in df.columns:
                merge_condition += " AND target.partition_date = source.partition_date"
            
            # Thực hiện Upsert chống trùng lặp
            delta_table.alias("target") \
                .merge(df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            print(f"  └─ ✓ MERGE thành công dữ liệu xuống MinIO.")
                
        else:
            # 2. Nếu chưa tồn tại Delta log tại path, ghi dữ liệu trước rồi mới đăng ký metadata.
            writer = (
                df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
            )
            if "partition_date" in df.columns:
                writer = writer.partitionBy("partition_date")
            writer.save(target_path)
            print(f"  └─ ✓ Khởi tạo Delta Table tại MinIO thành công.")

        if sync_hive:
            sync_hive_delta_table(spark, hive_db, table_name, target_path)
            print(f"  └─ ✓ Metadata Hive đã trỏ tới Delta table.")

def sql_identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def quoted_identifier(name):
    return f"`{sql_identifier(name)}`"


def qualified_table_name(db_name, table_name):
    return f"{quoted_identifier(db_name)}.{quoted_identifier(table_name)}"


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def table_exists(spark, db_name, table_name):
    return spark.catalog.tableExists(f"{db_name}.{table_name}")


def describe_delta_detail(spark, hive_table_name):
    return spark.sql(f"DESCRIBE DETAIL {hive_table_name}").first().asDict()


def sync_hive_delta_table(spark, db_name, table_name, target_path):
    """
    Đăng ký Delta table vào Hive Metastore bằng LOCATION.

    Delta tự quản lý partition qua transaction log, nên không chạy MSCK REPAIR TABLE.
    """
    hive_table_name = qualified_table_name(db_name, table_name)
    target_location = target_path.rstrip("/")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {quoted_identifier(db_name)}")

    if not table_exists(spark, db_name, table_name):
        spark.sql(
            f"CREATE TABLE {hive_table_name} "
            f"USING DELTA LOCATION {sql_string(target_path)}"
        )
        return

    try:
        detail = describe_delta_detail(spark, hive_table_name)
    except Exception as exc:
        print(
            f"  └─ ⚠ Metadata Hive của {db_name}.{table_name} đang lỗi; "
            "thử cập nhật lại LOCATION."
        )
        spark.sql(f"ALTER TABLE {hive_table_name} SET LOCATION {sql_string(target_path)}")
        try:
            detail = describe_delta_detail(spark, hive_table_name)
        except Exception as repair_exc:
            raise RuntimeError(
                f"Bảng Hive {db_name}.{table_name} đã tồn tại nhưng không đọc được như Delta. "
                "Hãy kiểm tra metadata cũ trong metastore hoặc đổi --hive-db."
            ) from repair_exc

    table_format = str(detail.get("format", "")).lower()
    current_location = str(detail.get("location", "")).rstrip("/")

    if table_format and table_format != "delta":
        raise RuntimeError(
            f"Bảng Hive {db_name}.{table_name} đang dùng format '{table_format}', "
            "không phải Delta. Hãy đổi tên bảng/database hoặc dọn metadata cũ."
        )

    if current_location and current_location != target_location:
        print(
            f"  └─ ⚠ Metadata Hive đang trỏ tới {current_location}; "
            f"cập nhật sang {target_location}."
        )
        spark.sql(f"ALTER TABLE {hive_table_name} SET LOCATION {sql_string(target_path)}")

def optional_sql_identifier(stage_tables, table_name):
    name = stage_tables.get(table_name)
    return sql_identifier(name) if name else None

def process_source(spark, source, args):
    print(f"\n=== Xử lý nguồn {source.upper()} ===")
    
    # 1. Đọc và chuẩn hóa dữ liệu từ Bronze
    tables = collect_normalized(spark, source, args.bronze_base, args.date)
    if not tables:
        print(f"⚠ Không có dữ liệu hợp lệ cho nguồn {source}.")
        return

    for table_name, df in tables.items():
        print(f"  - {table_name}: sẵn sàng ghi {len(df.columns)} cột")

    # 2. Ghi trực tiếp xuống MinIO (Silver Layer) bằng Delta Lake
    write_delta_silver(tables, args.silver_base, DEDUPE_KEYS, args.hive_db, args.sync_hive)
    
    print(f"✔ Đã lưu {source.upper()} thành công vào MinIO (Silver Layer).")

def main():
    args = parse_args()
    sources = SUPPORTED_SOURCES if args.source == "all" else (args.source,)

    # Khởi tạo Spark
    spark = build_spark(f"BronzeToSilver_{args.source.upper()}", enable_hive_support=args.sync_hive)
    
    try:
        for source in sources:
            # Chỉ truyền spark, source và args
            process_source(spark, source, args)
    except Exception as e:
        print(f"❌ Lỗi trong quá trình xử lý: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\nĐã đóng Spark Session.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"❌ Lỗi xử lý Bronze -> PostgreSQL: {exc}")
        sys.exit(1)
