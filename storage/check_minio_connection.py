import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
import os
from io import BytesIO

from dotenv import load_dotenv


load_dotenv()

def test_minio_connection():
    endpoint_url = os.getenv('MINIO_ENDPOINT_URL', 'http://127.0.0.1:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    bucket_name = os.getenv('MINIO_BUCKET_NAME', 'bronze-lakehouse')

    if not access_key or not secret_key:
        raise SystemExit(
            "Thiếu MINIO_ACCESS_KEY hoặc MINIO_SECRET_KEY. "
            "Hãy sao chép .env.example thành .env và cập nhật credential."
        )

    MINIO_CONFIG = {
        'endpoint_url': endpoint_url,
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
        'region_name': 'us-east-1',
    }

    print("--- Đang kiểm tra kết nối tới MinIO ---")

    try:
        # 2. Khởi tạo Client
        s3 = boto3.client('s3', 
            **MINIO_CONFIG,
            config=Config(signature_version='s3v4')
        )

        # 3. Kiểm tra xem có thấy Bucket không
        print("[1/3] Đang thử liệt kê các Buckets...")
        response = s3.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        print(f"✔ Thành công! Các buckets hiện có: {buckets}")

        if bucket_name not in buckets:
            print(f"⚠ Cảnh báo: Không tìm thấy bucket '{bucket_name}'. Hãy tạo nó trên Web UI.")
            return

        # 4. Kiểm tra quyền Đọc (List Objects)
        print(f"[2/3] Đang quét nội dung trong '{bucket_name}'...")
        objs = s3.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in objs:
            print(f"✔ Đã tìm thấy {len(objs['Contents'])} objects.")
            # In ra 3 đường dẫn đầu tiên để kiểm tra Hive Partition
            print("Gợi ý 3 file đầu tiên:")
            for o in objs['Contents'][:3]:
                print(f"  - {o['Key']}")
        else:
            print("ℹ Bucket đang trống (hoặc metadata chưa đồng bộ).")

        # 5. Thử ghi một file nháp (Test Write)
        print("[3/3] Đang thử ghi file test vào MinIO...")
        test_data = BytesIO(b"MinIO connection test")
        s3.upload_fileobj(test_data, bucket_name, 'test/connection_test.txt')
        print("✔ Ghi file thành công! Hãy kiểm tra Web UI xem có folder 'test/' chưa.")

    except EndpointConnectionError:
        print("✘ LỖI: Không thể kết nối tới Endpoint (9000). MinIO đã chạy chưa?")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidAccessKeyId':
            print("✘ LỖI: Sai Access Key ID.")
        elif error_code == 'SignatureDoesNotMatch':
            print("✘ LỖI: Sai Secret Access Key.")
        else:
            print(f"✘ LỖI S3: {e}")
    except Exception as e:
        print(f"✘ LỖI KHÔNG XÁC ĐỊNH: {e}")

if __name__ == "__main__":
    test_minio_connection()
