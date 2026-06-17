from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount
import os

# Đường dẫn thư mục dự án trên máy host (server hoặc local)
HOST_WORKSPACE = os.getenv('HOST_WORKSPACE_PATH', '/home/ubuntu/ecommerce-lakehouse')

default_args = {
    'owner': 'hai_data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tiki_ecommerce_ingestion_sequential',
    default_args=default_args,
    description='Pipeline tự động cào tuần tự dữ liệu Tiki theo chu kỳ 15 phút',
    schedule='*/15 * * * *',  # Cứ đúng mỗi 20 phút gọi 1 lần
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task sử dụng DockerOperator để cô lập môi trường chạy script Python cào dữ liệu
    run_tiki_crawler = DockerOperator(
        task_id='run_tiki_crawler_script',
        image='ecommerce-crawler:latest',
        command='python /app/ingestion/batch/main.py --limit_pages 1',
        network_mode='ecommerce-lakehouse_default',
        auto_remove='force',
        mount_tmp_dir=False,  # Tắt mount thư mục tạm từ host (bắt buộc khi chạy Airflow trong Docker)
        mounts=[
            Mount(
                source=HOST_WORKSPACE,
                target='/app',
                type='bind'
            )
        ],

        environment={
            'BROWSERLESS_URL': 'http://browserless_chrome:3000/webdriver',
            'MINIO_ENDPOINT_URL': 'http://minio:9000',
            'MINIO_ACCESS_KEY': 'admin',
            'MINIO_SECRET_KEY': 'password123',
            'MINIO_BUCKET_NAME': 'bronze-lakehouse'
        },
    )

    # Task sử dụng DockerOperator để chuẩn hóa dữ liệu thô sang Delta Lake Silver
    run_tiki_silver_processing = DockerOperator(
        task_id='run_tiki_silver_processing_script',
        image='doanhai2005/pandas-processor:1.2',
        command='python /app/processing/jobs/tiki_bronze_to_silver_real.py --date {{ ds }}',
        network_mode='ecommerce-lakehouse_default',
        auto_remove='force',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=HOST_WORKSPACE,
                target='/app',
                type='bind'
            )
        ],
        environment={
            'MINIO_ENDPOINT_URL': 'http://minio:9000',
            'MINIO_ACCESS_KEY': 'admin',
            'MINIO_SECRET_KEY': 'password123',
            'MINIO_BUCKET_NAME': 'bronze-lakehouse',
            'SILVER_BASE_PATH': 's3a://silver-lakehouse/real_data',
            'SILVER_HIVE_DATABASE': 'silver_real',
            'HIVE_SITE_PATH': '/app/hive-site.xml',
            'HIVE_METASTORE_JDBC_URL': 'jdbc:postgresql://postgres:5432/postgres_metastore',
            'HIVE_METASTORE_JDBC_USER': 'postgres',
            'HIVE_METASTORE_JDBC_PASSWORD': 'postgres',
            'SPARK_WAREHOUSE_DIR': 's3a://silver-lakehouse/warehouse/'
        },
    )

    run_tiki_crawler >> run_tiki_silver_processing