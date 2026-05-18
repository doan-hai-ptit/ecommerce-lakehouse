from providers.tiki_client import TikiApiClient
import argparse
import psycopg2 # Thư viện kết nối Postgres
import os

# Cấu hình kết nối tới Postgres Metastore của bạn
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "postgres_metastore"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "port": int(os.getenv("DB_PORT", 5432))
}

def get_next_page_from_db(category_id):
    """Hàm lấy số trang tiếp theo cần cào từ Postgres"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Đọc số trang hiện tại
    cur.execute("SELECT next_page FROM crawler_state WHERE category_id = %s;", (category_id,))
    row = cur.fetchone()
    
    if row is None:
        # Nếu chưa có danh mục này, tự động chèn mới khởi đầu từ trang 1
        cur.execute("INSERT INTO crawler_state (category_id, next_page) VALUES (%s, 1);", (category_id,))
        conn.commit()
        current_page = 1
    else:
        current_page = row[0]
        
    cur.close()
    conn.close()
    return current_page

def update_next_page_in_db(category_id, current_end_page):
    """Hàm cập nhật số trang tiếp theo vào Postgres sau khi cào xong"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Trang tiếp theo sẽ bằng trang vừa kết thúc cộng thêm 1
    next_page = current_end_page + 1
    
    cur.execute("UPDATE crawler_state SET next_page = %s WHERE category_id = %s;", (next_page, category_id))
    conn.commit()
    
    cur.close()
    conn.close()
    print(f"[Metadata] Đã cập nhật trạng thái trong Postgres: Lần tới sẽ cào từ trang {next_page}")

def main():
    print("=== Hệ thống Ingestion Tự động Tăng Trang - ecommerce-lakehouse ===")
    
    parser = argparse.ArgumentParser(description="Tiki Dynamic Crawler")
    parser.add_argument('--category', type=int, default=1846, help='ID danh mục cần cào')
    parser.add_argument('--limit_pages', type=int, default=1, help='Số lượng trang muốn cào mỗi lần chạy')
    args = parser.parse_args()
    
    CATEGORY_ID = args.category
    LIMIT_PAGES = args.limit_pages # Mỗi lần chạy muốn cào bao nhiêu trang (Mặc định: 1 trang)
    
    # 1. Tự động lấy số trang bắt đầu từ Postgres
    START = get_next_page_from_db(CATEGORY_ID)
    END = START + LIMIT_PAGES - 1
    
    client = TikiApiClient()
    print(f"\n[Bắt đầu Ingestion] Danh mục: {CATEGORY_ID} | Từ trang {START} đến {END}")
    
    try:
        # 2. Tiến hành cào dữ liệu đổ vào MinIO
        client.crawl_all(category_id=CATEGORY_ID, start_page=START, end_page=END)
        print("✔ Cào dữ liệu và lưu vào MinIO thành công!")
        
        # 3. Cào thành công thì mới cập nhật số trang mới vào Postgres
        update_next_page_in_db(CATEGORY_ID, END)
        
    except Exception as e:
        print(f"❌ Lỗi trong quá trình cào dữ liệu: {e}")
        print("⚠ Không cập nhật số trang trong Postgres để lần sau Airflow chạy lại trang này.")

if __name__ == "__main__":
    main()