from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def read_silver(spark, silver_base, table_name):
    path = f"{silver_base.rstrip('/')}/{table_name}"
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        print(f"⚠️  Không thể đọc bảng Silver '{table_name}' tại '{path}' (Có thể bảng chưa tồn tại): {e}")
        return None


def build_dim_platforms(spark, silver_base, primary_df=None):
    platforms = primary_df if primary_df is not None else read_silver(spark, silver_base, "platforms")
    if not platforms:
        return None

    return platforms.select(
        "platform_id",
        "platform_code",
        "platform_name",
        "base_url",
        "is_active",
        "created_at",
        "updated_at"
    )


def build_dim_brands(spark, silver_base, primary_df=None):
    brands = primary_df if primary_df is not None else read_silver(spark, silver_base, "brands")
    platforms = read_silver(spark, silver_base, "platforms")
    if not brands:
        return None

    if platforms:
        joined = brands.join(platforms, "platform_id", "left")
        return joined.select(
            brands.brand_id,
            brands.platform_id,
            platforms.platform_code,
            platforms.platform_name,
            brands.platform_brand_id,
            brands.brand_name,
            brands.country,
            brands.is_official,
            brands.created_at,
            brands.updated_at
        )
    else:
        return brands.select(
            "brand_id",
            "platform_id",
            F.lit(None).cast(StringType()).alias("platform_code"),
            F.lit(None).cast(StringType()).alias("platform_name"),
            "platform_brand_id",
            "brand_name",
            "country",
            "is_official",
            "created_at",
            "updated_at"
        )


def build_dim_sellers(spark, silver_base, primary_df=None):
    sellers = primary_df if primary_df is not None else read_silver(spark, silver_base, "sellers")
    platforms = read_silver(spark, silver_base, "platforms")
    if not sellers:
        return None

    if platforms:
        joined = sellers.join(platforms, "platform_id", "left")
        return joined.select(
            sellers.seller_id,
            sellers.platform_id,
            platforms.platform_code,
            platforms.platform_name,
            sellers.platform_seller_id,
            sellers.seller_name,
            sellers.city,
            sellers.province,
            sellers.follower_count,
            sellers.is_official_store,
            sellers.status,
            sellers.created_at,
            sellers.updated_at
        )
    else:
        return sellers.select(
            "seller_id",
            "platform_id",
            F.lit(None).cast(StringType()).alias("platform_code"),
            F.lit(None).cast(StringType()).alias("platform_name"),
            "platform_seller_id",
            "seller_name",
            "city",
            "province",
            "follower_count",
            "is_official_store",
            "status",
            "created_at",
            "updated_at"
        )


def build_dim_customers(spark, silver_base, primary_df=None):
    customers = primary_df if primary_df is not None else read_silver(spark, silver_base, "customers")
    platforms = read_silver(spark, silver_base, "platforms")
    addresses = read_silver(spark, silver_base, "customer_addresses")
    if not customers:
        return None

    if addresses:
        window_spec = Window.partitionBy("customer_id").orderBy(
            F.col("is_default").desc(),
            F.col("updated_at").desc()
        )
        primary_addr = addresses.withColumn("rn", F.row_number().over(window_spec)) \
                                .filter(F.col("rn") == 1) \
                                .select(
                                    "customer_id",
                                    F.col("address_line").alias("primary_address_line"),
                                    F.col("ward").alias("primary_ward"),
                                    F.col("district").alias("primary_district"),
                                    F.col("city").alias("primary_city"),
                                    F.col("province").alias("primary_province"),
                                    F.col("country").alias("primary_country")
                                )
    else:
        primary_addr = None

    joined = customers
    if platforms:
        joined = joined.join(platforms, "platform_id", "left")
    else:
        joined = joined.withColumn("platform_code", F.lit(None).cast(StringType())) \
                       .withColumn("platform_name", F.lit(None).cast(StringType()))

    if primary_addr:
        joined = joined.join(primary_addr, "customer_id", "left")
    else:
        joined = joined.withColumn("primary_address_line", F.lit(None).cast(StringType())) \
                       .withColumn("primary_ward", F.lit(None).cast(StringType())) \
                       .withColumn("primary_district", F.lit(None).cast(StringType())) \
                       .withColumn("primary_city", F.lit(None).cast(StringType())) \
                       .withColumn("primary_province", F.lit(None).cast(StringType())) \
                       .withColumn("primary_country", F.lit(None).cast(StringType()))

    return joined.select(
        customers.customer_id,
        customers.platform_id,
        F.col("platform_code"),
        F.col("platform_name"),
        customers.platform_customer_id,
        customers.full_name,
        customers.email,
        customers.phone_number,
        customers.gender,
        customers.date_of_birth,
        customers.status,
        F.col("primary_address_line"),
        F.col("primary_ward"),
        F.col("primary_district"),
        F.col("primary_city"),
        F.col("primary_province"),
        F.col("primary_country"),
        customers.created_at,
        customers.updated_at
    )


def build_dim_products(spark, silver_base, primary_df=None):
    products = primary_df if primary_df is not None else read_silver(spark, silver_base, "products")
    sellers = read_silver(spark, silver_base, "sellers")
    categories = read_silver(spark, silver_base, "categories")
    brands = read_silver(spark, silver_base, "brands")
    if not products:
        return None

    joined = products
    if sellers:
        joined = joined.join(sellers.select("seller_id", "seller_name"), "seller_id", "left")
    else:
        joined = joined.withColumn("seller_name", F.lit(None).cast(StringType()))

    if categories:
        joined = joined.join(categories.select("category_id", "category_name"), "category_id", "left")
    else:
        joined = joined.withColumn("category_name", F.lit(None).cast(StringType()))

    if brands:
        joined = joined.join(brands.select("brand_id", "brand_name"), "brand_id", "left")
    else:
        joined = joined.withColumn("brand_name", F.lit(None).cast(StringType()))

    return joined.select(
        products.product_id,
        products.platform_product_id,
        products.seller_id,
        F.col("seller_name"),
        products.category_id,
        F.col("category_name"),
        products.brand_id,
        F.col("brand_name"),
        products.product_name,
        products.description,
        products.status,
        products.is_authentic,
        products.published_at,
        products.created_at,
        products.updated_at
    )


def build_dim_product_variants(spark, silver_base, primary_df=None):
    variants = primary_df if primary_df is not None else read_silver(spark, silver_base, "product_variants")
    products = read_silver(spark, silver_base, "products")
    if not variants:
        return None

    if products:
        joined = variants.join(products.select("product_id", "product_name"), "product_id", "left")
        return joined.select(
            variants.variant_id,
            variants.product_id,
            F.col("product_name"),
            variants.platform_variant_id,
            variants.sku,
            variants.variant_name,
            variants.original_price,
            variants.sale_price,
            variants.weight_gram,
            variants.status,
            variants.created_at,
            variants.updated_at
        )
    else:
        return variants.select(
            "variant_id",
            "product_id",
            F.lit(None).cast(StringType()).alias("product_name"),
            "platform_variant_id",
            "sku",
            "variant_name",
            "original_price",
            "sale_price",
            "weight_gram",
            "status",
            "created_at",
            "updated_at"
        )


BUILDERS = {
    "dim_platforms": build_dim_platforms,
    "dim_brands": build_dim_brands,
    "dim_sellers": build_dim_sellers,
    "dim_customers": build_dim_customers,
    "dim_products": build_dim_products,
    "dim_product_variants": build_dim_product_variants,
}

PRIMARY_KEYS = {
    "dim_platforms": ["platform_id"],
    "dim_brands": ["brand_id"],
    "dim_sellers": ["seller_id"],
    "dim_customers": ["customer_id"],
    "dim_products": ["product_id"],
    "dim_product_variants": ["variant_id"],
}

PRIMARY_SILVER_TABLES = {
    "dim_platforms": "platforms",
    "dim_brands": "brands",
    "dim_sellers": "sellers",
    "dim_customers": "customers",
    "dim_products": "products",
    "dim_product_variants": "product_variants",
}
