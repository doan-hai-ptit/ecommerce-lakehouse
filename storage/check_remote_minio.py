import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import load_dotenv
import os

def check_remote_minio():
    # Load các biến môi trường từ file .env ở thư mục gốc của dự án
    load_dotenv()
    
    MINIO_ENDPOINT = os.getenv("REMOTE_MINIO_ENDPOINT")
    ACCESS_KEY = os.getenv("REMOTE_MINIO_ACCESS_KEY")
    SECRET_KEY = os.getenv("REMOTE_MINIO_SECRET_KEY")

    print(f"--- Đang kiểm tra kết nối tới MinIO từ xa ---")
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Access Key (User): {ACCESS_KEY}")
    print(f"-------------------------------------------")

    if not ACCESS_KEY or not SECRET_KEY:
        print("✘ LỖI: Thiếu cấu hình REMOTE_MINIO_ACCESS_KEY hoặc REMOTE_MINIO_SECRET_KEY trong file .env!")
        return

    try:
        # Khởi tạo S3 Client kết nối tới MinIO
        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # 1. Thử liệt kê danh sách buckets (Kiểm tra quyền kết nối & đọc)
        print("[1/2] Đang gửi yêu cầu liệt kê các Buckets...")
        response = s3.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        print(f"✔ Kết nối thành công! Các buckets hiện có trên server: {buckets}")

        # 2. Thử ghi thử 1 file nháp lên bucket đầu tiên (nếu có)
        if buckets:
            target_bucket = buckets[0]
            print(f"[2/2] Đang thử ghi file test lên bucket '{target_bucket}'...")
            
            test_key = "test_connection_remote.txt"
            test_content = b"Connect successful from remote client!"
            
            s3.put_object(
                Bucket=target_bucket,
                Key=test_key,
                Body=test_content
            )
            print(f"✔ Ghi file test thành công lên '{target_bucket}/{test_key}'!")
            
            # Xóa file test sau khi ghi thành công để dọn dẹp
            s3.delete_object(Bucket=target_bucket, Key=test_key)
            print("✔ Đã dọn dẹp file test thành công.")
        else:
            print("⚠ Cảnh báo: Hiện chưa có bucket nào trên MinIO của bạn. Hãy tạo 1 bucket trên Web UI.")

    except EndpointConnectionError:
        print(f"✘ LỖI KẾT NỐI: Không thể kết nối tới {MINIO_ENDPOINT}. Hãy kiểm tra xem:")
        print("  1. URL endpoint có chính xác và truy cập được từ bên ngoài không.")
        print("  2. Dịch vụ MinIO trên server đã được khởi động và đang chạy chưa.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidAccessKeyId':
            print("✘ LỖI XÁC THỰC: Sai Access Key ID (User).")
        elif error_code == 'SignatureDoesNotMatch':
            print("✘ LỖI XÁC THỰC: Sai Secret Access Key (Password).")
        else:
            print(f"✘ LỖI S3: {e}")
    except Exception as e:
        print(f"✘ LỖI KHÔNG XÁC ĐỊNH: {e}")

if __name__ == "__main__":
    check_remote_minio()
