import psycopg2
from faker import Faker
import time
import random

# Khởi tạo thư viện Faker
fake = Faker()

# Thông tin kết nối lấy từ docker-compose.yml
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce_db"
DB_USER = "admin"
DB_PASS = "change_me"

def get_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        print(f"Lỗi kết nối database: {e}")
        return None

def setup_database():
    conn = get_connection()
    if conn is None: return
    
    try:
        cursor = conn.cursor()
        # Tạo bảng orders nếu chưa tồn tại
        create_table_query = """
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            user_id INT,
            product_id INT,
            amount INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Đã kiểm tra/tạo bảng 'orders' thành công!")
    except Exception as e:
        print(f"Lỗi khi tạo bảng: {e}")
    finally:
        cursor.close()
        conn.close()

def simulate_streaming_data():
    conn = get_connection()
    if conn is None: return

    cursor = conn.cursor()
    print("Bắt đầu chèn dữ liệu giả. Nhấn Ctrl+C để dừng...")
    try:
        while True:
            # Sinh dữ liệu ngẫu nhiên
            user_id = random.randint(1, 1000)
            product_id = random.randint(1, 500)
            amount = random.randint(10000, 5000000) # Số tiền (VNĐ)

            # Insert vào database
            insert_query = """
                INSERT INTO orders (user_id, product_id, amount)
                VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, (user_id, product_id, amount))
            conn.commit()
            
            print(f"Đã chèn Order: User {user_id} mua Product {product_id} với giá {amount}")
            
            # Tạm dừng 1 giây để giả lập luồng dữ liệu (streaming)
            time.sleep(1) 
            
    except KeyboardInterrupt:
        print("\nĐã nhận lệnh dừng (Ctrl+C). Kết thúc luồng dữ liệu.")
    except Exception as e:
        print(f"Lỗi trong quá trình chèn dữ liệu: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_database()
    simulate_streaming_data()