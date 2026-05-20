from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

default_args = {
    'owner': 'hai_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 15), # Chạy từ ngày hôm qua
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tiki_ecommerce_ingestion_category_1520',
    default_args=default_args,
    description='Pipeline tự động cào dữ liệu Tiki theo chu kỳ 20 phút',
    schedule='*/20 * * * *',  # Cứ đúng mỗi 20 phút gọi 1 lần
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task sử dụng DockerOperator để cô lập môi trường chạy script Python cào dữ liệu
    run_tiki_crawler = DockerOperator(
        task_id='run_tiki_crawler_script',
        image='ecommerce-crawler:latest',
        command='python /app/ingestion/batch/main.py --category 1520 --limit_pages 1',
        network_mode='ecommerce-lakehouse_default',
        auto_remove='force',

        environment={
            'BROWSERLESS_URL': 'http://browserless_chrome:3000/webdriver',
            'MINIO_ENDPOINT_URL': 'http://minio:9000',
            'MINIO_ACCESS_KEY': 'admin',
            'MINIO_SECRET_KEY': 'password123',
            'MINIO_BUCKET_NAME': 'bronze-lakehouse',
            
            'DB_HOST': 'postgres_metastore', 
            'DB_PORT': '5432',
            'DB_USER': 'postgres',
            'DB_PASSWORD': 'postgres',
            'DB_NAME': 'postgres_metastore'
        },
    )

    run_tiki_crawler