import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load environment variables from .env file if present
load_dotenv()

def get_env(*names, default=None):
    """Get the first non-empty environment variable from the list of names."""
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default

def get_spark_session(app_name="EcommerceLakehouse", enable_hive_support=False, log_level="WARN"):
    """
    Build and return a unified SparkSession configured for MinIO (S3), Delta Lake, and optional Hive Metastore.
    """
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
        
        # 1. Local Warehouse Management & Resource Optimization
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.hadoop.hive.metastore.warehouse.dir", warehouse_dir)
        .config("spark.sql.hive.manageFilesourceTables", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.row-group-size.bytes", "16777216")
        
        # 2. S3A / MinIO Configuration
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.hive.metastore.authorization.storage.checks", "false")
       
        # 3. Disable fallback stats to optimize scans
        .config("spark.sql.statistics.fallBackToHdfs", "false")

        # 4. Delta Configuration
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.jsonGenerator.ignoreNullFields", "false")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    )
    
    if enable_hive_support:
        builder = builder.enableHiveSupport()
        
    spark = builder.getOrCreate()
    
    # Set Spark log level
    spark.sparkContext.setLogLevel(log_level)
    
    return spark
