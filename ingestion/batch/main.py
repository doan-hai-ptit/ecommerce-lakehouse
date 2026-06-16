from providers.tiki_client import TikiApiClient
import argparse
import os

def get_next_page_from_db(category_id):
    """Hàm lấy số trang tiếp theo cần cào từ Postgres (chỉ gọi khi use_db=True)"""
    import psycopg2
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("DB_NAME", "postgres_metastore"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "port": int(os.getenv("DB_PORT", 5432))
    }
    conn = psycopg2.connect(**db_config)
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
    """Hàm cập nhật số trang tiếp theo vào Postgres sau khi cào xong (chỉ gọi khi use_db=True)"""
    import psycopg2
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("DB_NAME", "postgres_metastore"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "port": int(os.getenv("DB_PORT", 5432))
    }
    conn = psycopg2.connect(**db_config)
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
    parser.add_argument('--start_page', type=int, default=1, help='Trang bắt đầu cào (chỉ dùng khi không dùng DB)')
    parser.add_argument('--output', type=str, default='tiki_data.txt', help='Đường dẫn file .txt để lưu dữ liệu')
    parser.add_argument('--use_db', action='store_true', help='Lưu dữ liệu vào database (Postgres & MinIO) thay vì file .txt')
    args = parser.parse_args()
    
    CATEGORY_ID = args.category
    LIMIT_PAGES = args.limit_pages
    USE_DB = args.use_db
    OUTPUT_FILE = args.output
    
    # 1. Tự động lấy hoặc chỉ định số trang bắt đầu
    if USE_DB:
        try:
            START = get_next_page_from_db(CATEGORY_ID)
        except Exception as e:
            print(f"❌ Lỗi kết nối Postgres khi lấy trạng thái: {e}")
            print("⚠ Sẽ chuyển sang chế độ không dùng database và bắt đầu từ --start_page")
            USE_DB = False
            START = args.start_page
    else:
        START = args.start_page
        
    END = START + LIMIT_PAGES - 1
    
    client = TikiApiClient()
    if USE_DB:
        print(f"\n[Bắt đầu Ingestion - Database Mode] Danh mục: {CATEGORY_ID} | Từ trang {START} đến {END}")
    else:
        print(f"\n[Bắt đầu Ingestion - Local TXT Mode] Danh mục: {CATEGORY_ID} | Từ trang {START} đến {END} | Lưu vào: {OUTPUT_FILE}")
    
    try:
        # 2. Tiến hành cào dữ liệu
        client.crawl_all(
            category_id=CATEGORY_ID, 
            start_page=START, 
            end_page=END, 
            use_db=USE_DB, 
            output_file=OUTPUT_FILE
        )
        
        if USE_DB:
            print("✔ Cào dữ liệu và lưu vào MinIO thành công!")
            # 3. Cào thành công thì mới cập nhật số trang mới vào Postgres
            update_next_page_in_db(CATEGORY_ID, END)
        else:
            print(f"✔ Cào dữ liệu và lưu vào file {OUTPUT_FILE} thành công!")
        
    except Exception as e:
        print(f"❌ Lỗi trong quá trình cào dữ liệu: {e}")
        if USE_DB:
            print("⚠ Không cập nhật số trang trong Postgres để lần sau Airflow chạy lại trang này.")

if __name__ == "__main__":
    main()