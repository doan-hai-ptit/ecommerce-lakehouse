import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
import os

def test_minio_connection():
    # 1. Thông số cấu hình - Hải kiểm tra kỹ Access Key và Secret Key ở đây
    MINIO_CONFIG = {
        'endpoint_url': 'http://127.0.0.1:9000', # Dùng 127.0.0.1 để tránh lỗi IPv6 của localhost
        'aws_access_key_id': 'REDACTED_MINIO_ACCESS_KEY',  # Thay bằng Access Key mới tạo
        'aws_secret_access_key': 'REDACTED_MINIO_SECRET_KEY', # Thay bằng Secret Key mới tạo
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

        if 'bronze-lakehouse' not in buckets:
            print("⚠ Cảnh báo: Không tìm thấy bucket 'bronze-lakehouse'. Hãy tạo nó trên Web UI.")
            return

        # 4. Kiểm tra quyền Đọc (List Objects)
        print(f"[2/3] Đang quét nội dung trong 'bronze-lakehouse'...")
        objs = s3.list_objects_v2(Bucket='bronze-lakehouse')
        
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
        test_file = "connection_test.txt"
        with open(test_file, "w") as f:
            f.write("Test connection from PTIT student")
        
        s3.upload_file(test_file, 'bronze-lakehouse', 'test/connection_test.txt')
        print("✔ Ghi file thành công! Hãy kiểm tra Web UI xem có folder 'test/' chưa.")
        
        # Dọn dẹp file nháp ở local
        os.remove(test_file)

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