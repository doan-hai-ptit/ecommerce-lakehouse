from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount
import os


HOST_WORKSPACE = os.getenv("HOST_WORKSPACE_PATH", "/home/ubuntu/ecommerce-lakehouse")

SHOPEE_KEYWORD = os.getenv("SHOPEE_AIRFLOW_KEYWORD", "dien thoai")
SHOPEE_START_PAGE = os.getenv("SHOPEE_AIRFLOW_START_PAGE", "0")
SHOPEE_END_PAGE = os.getenv("SHOPEE_AIRFLOW_END_PAGE", "0")
SHOPEE_REVIEW_PRODUCTS_LIMIT = os.getenv("SHOPEE_AIRFLOW_REVIEW_PRODUCTS_LIMIT", "3")
SHOPEE_REVIEW_PAGES = os.getenv("SHOPEE_AIRFLOW_REVIEW_PAGES", "1")
SHOPEE_FETCH_MODE = os.getenv("SHOPEE_AIRFLOW_FETCH_MODE", "api")


default_args = {
    "owner": "hai_data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 23),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "shopee_ecommerce_ingestion_keyword",
    default_args=default_args,
    description="Pipeline cào dữ liệu Shopee theo keyword và tải raw JSON lên MinIO Bronze",
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    run_shopee_crawler = DockerOperator(
        task_id="run_shopee_crawler_script",
        image="ecommerce-crawler:latest",
        command=(
            "python /app/ingestion/batch/main_shopee.py "
            f"--keyword \"{SHOPEE_KEYWORD}\" "
            f"--start_page {SHOPEE_START_PAGE} "
            f"--end_page {SHOPEE_END_PAGE} "
            f"--review_products_limit {SHOPEE_REVIEW_PRODUCTS_LIMIT} "
            f"--review_pages {SHOPEE_REVIEW_PAGES} "
            f"--fetch_mode {SHOPEE_FETCH_MODE} "
            "--driver browserless "
            "--headless "
            "--no_open_search_page"
        ),
        network_mode="ecommerce-lakehouse_default",
        auto_remove="force",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=HOST_WORKSPACE,
                target="/app",
                type="bind",
            )
        ],
        environment={
            "BROWSERLESS_URL": "http://browserless_chrome:3000/webdriver",
            "MINIO_ENDPOINT_URL": "http://minio:9000",
            "MINIO_ACCESS_KEY": "admin",
            "MINIO_SECRET_KEY": "password123",
            "MINIO_BUCKET_NAME": "bronze-lakehouse",
            "SHOPEE_PAGE_SIZE": os.getenv("SHOPEE_PAGE_SIZE", "60"),
            "SHOPEE_MAX_REVIEW_PRODUCTS": os.getenv("SHOPEE_MAX_REVIEW_PRODUCTS", "3"),
            "SHOPEE_REVIEW_PAGES": os.getenv("SHOPEE_REVIEW_PAGES", "1"),
            "SHOPEE_MIN_DELAY_SECONDS": os.getenv("SHOPEE_MIN_DELAY_SECONDS", "3"),
            "SHOPEE_MAX_DELAY_SECONDS": os.getenv("SHOPEE_MAX_DELAY_SECONDS", "8"),
            "SHOPEE_MAX_RETRIES": os.getenv("SHOPEE_MAX_RETRIES", "3"),
            "SHOPEE_HOME_WAIT_SECONDS": os.getenv("SHOPEE_HOME_WAIT_SECONDS", "8"),
            "SHOPEE_SEARCH_WAIT_SECONDS": os.getenv("SHOPEE_SEARCH_WAIT_SECONDS", "8"),
            "SHOPEE_FETCH_MODE": SHOPEE_FETCH_MODE,
            "SHOPEE_COOKIE": os.getenv("SHOPEE_COOKIE", ""),
        },
    )
