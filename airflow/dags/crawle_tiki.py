from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'doanhai',
    'start_date': datetime(2026, 5, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tiki_auto_increment_crawl',
    default_args=default_args,
    description='DAG cào dữ liệu Tiki tự động tăng trang sau mỗi lần chạy',
    schedule='*/30 * * * *', # Chạy mỗi 30 phút một lần
    catchup=False,
    tags=['ingestion', 'tiki'],
) as dag:

    # Chỉ cần truyền danh mục muốn cào (Ví dụ: Đồ gia dụng 1883)
    # Và giới hạn mỗi 30 phút cào 1 trang độc nhất (--limit_pages 1)
    run_crawler = BashOperator(
        task_id='execute_auto_crawl',
        # Thêm đường dẫn ingestion/batch/ vào trước main.py
        bash_command='cd /opt/airflow/ecommerce-lakehouse && source venv/bin/activate && python3 ingestion/batch/main.py --category 1882 --limit_pages 1'
    )