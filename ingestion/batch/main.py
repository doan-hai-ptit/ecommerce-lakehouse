from providers.tiki_client import TikiApiClient
import argparse
import os
import json

DEFAULT_TIKI_CATEGORIES = {
    1846: "Laptop - Máy Vi Tính - Linh kiện",
    1520: "Điện Thoại - Máy Tính Bảng",
    1789: "Đồ Chơi - Mẹ & Bé",
    2549: "Đồ Gia Dụng",
    8322: "Nhà Cửa - Đời Sống",
    915: "Thiết Bị Số - Phụ Kiện Số",
}

STATE_FILE = "crawler_state.txt"

def get_next_page_from_file(category_id, state_file=STATE_FILE):
    """Hàm lấy số trang tiếp theo cần cào từ file text"""
    if not os.path.exists(state_file):
        return 1
    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            state = json.load(f)
        val = state.get(str(category_id), 1)
        if isinstance(val, dict):
            return val.get("next_page", 1)
        return val  # Hỗ trợ định dạng cũ (chỉ lưu số trang trực tiếp)
    except Exception as e:
        print(f"⚠ Không đọc được file trạng thái {state_file} ({e}). Mặc định cào từ trang 1.")
        return 1

def update_next_page_in_file(category_id, current_end_page, category_name="Unknown", state_file=STATE_FILE):
    """Hàm cập nhật số trang tiếp theo và tên loại sản phẩm vào file text"""
    state = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
        except Exception:
            pass
    
    next_page = current_end_page + 1
    state[str(category_id)] = {
        "next_page": next_page,
        "category_name": category_name
    }
    
    try:
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        print(f"[Metadata] Đã cập nhật trạng thái vào file: {state_file} (Danh mục: {category_name}, Lần tới sẽ cào từ trang {next_page})")
    except Exception as e:
        print(f"❌ Lỗi ghi file trạng thái {state_file}: {e}")

def run_single_category(category_id, category_name, limit_pages, start_page_override, state_file=STATE_FILE):
    """Cào một danh mục duy nhất"""
    state = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
        except Exception:
            pass

    if start_page_override is not None:
        START = start_page_override
    else:
        cat_state = state.get(str(category_id), 1)
        if isinstance(cat_state, dict):
            START = cat_state.get("next_page", 1)
        else:
            START = cat_state

    # Cập nhật thông tin danh mục trong state
    if str(category_id) not in state or not isinstance(state[str(category_id)], dict):
        state[str(category_id)] = {
            "next_page": START,
            "category_name": category_name
        }
    else:
        state[str(category_id)]["category_name"] = category_name

    if START > 40 and start_page_override is None:
        print(f"⚠ Danh mục {category_id} ({category_name}) đã hoàn thành cào 40 trang. Không cào thêm.")
        return

    END = START + limit_pages - 1
    if END > 40:
        END = 40
        print(f"⚠ Giới hạn cào tối đa 40 trang. Điều chỉnh trang kết thúc về trang 40.")
        if START > END:
            print("✔ Đã đạt giới hạn 40 trang. Không cần cào thêm.")
            return

    pages_to_crawl = END - START + 1
    print(f"\n[Cào đơn danh mục] Danh mục: {category_id} ({category_name})")
    print(f" - Từ trang {START} đến {END} (Số lượng: {pages_to_crawl} trang)")

    client = TikiApiClient()
    client.crawl_all(category_id=category_id, start_page=START, end_page=END)

    # Cập nhật lại state
    next_page_new = END + 1
    state[str(category_id)]["next_page"] = next_page_new
    
    with open(state_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4, ensure_ascii=False)
    print(f"[Metadata] Đã cập nhật {category_name} -> next_page: {next_page_new}")

def run_sequential_crawler(limit_pages, state_file=STATE_FILE):
    """Cào tuần tự các danh mục trong state file đến khi đủ 40 trang mỗi danh mục"""
    state = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
        except Exception:
            pass
            
    # Khởi tạo danh mục mặc định nếu file trạng thái trống
    if not state:
        state = {
            str(cat_id): {
                "next_page": 1,
                "category_name": name
            } for cat_id, name in DEFAULT_TIKI_CATEGORIES.items()
        }
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        print(f"[*] Đã khởi tạo danh sách danh mục mặc định vào file: {state_file}")

    client = TikiApiClient()
    pages_left = limit_pages
    
    while pages_left > 0:
        # Tìm danh mục đầu tiên chưa cào đủ 40 trang (next_page <= 40)
        active_cat_id = None
        active_cat_info = None
        for cat_id, info in state.items():
            next_page = info.get("next_page", 1)
            if next_page <= 40:
                active_cat_id = cat_id
                active_cat_info = info
                break
                
        if not active_cat_id:
            print("\n✔ Tất cả danh mục trong file trạng thái đã hoàn thành cào 40 trang!")
            break
            
        cat_id_int = int(active_cat_id)
        cat_name = active_cat_info.get("category_name", f"Category_{active_cat_id}")
        start_page = active_cat_info.get("next_page", 1)
        
        # Số trang tối đa có thể cào cho danh mục này để đạt giới hạn 40
        max_crawlabes = 41 - start_page
        pages_to_crawl = min(pages_left, max_crawlabes)
        end_page = start_page + pages_to_crawl - 1
        
        print(f"\n[Cào tuần tự] Đang cào danh mục: {cat_id_int} ({cat_name})")
        print(f" - Từ trang {start_page} đến {end_page} (Số lượng: {pages_to_crawl} trang)")
        
        client.crawl_all(
            category_id=cat_id_int,
            start_page=start_page,
            end_page=end_page
        )
        
        # Cập nhật trạng thái cho danh mục này
        next_page_new = end_page + 1
        state[active_cat_id]["next_page"] = next_page_new
        
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
            
        print(f"[Metadata] Đã cập nhật {cat_name} -> next_page: {next_page_new}")
        
        if next_page_new > 40:
            print(f"✔ Danh mục '{cat_name}' đã cào đủ 40 trang. Sẽ tự động chuyển sang danh mục tiếp theo.")
            
        pages_left -= pages_to_crawl

def main():
    print("=== Hệ thống Ingestion Tự động Tăng Trang - ecommerce-lakehouse ===")
    
    parser = argparse.ArgumentParser(description="Tiki Dynamic Crawler")
    parser.add_argument('--category', type=int, default=None, help='ID danh mục cần cào (để trống để chạy tuần tự tất cả danh mục)')
    parser.add_argument('--category_name', type=str, default=None, help='Tên loại sản phẩm (chỉ dùng khi truyền --category)')
    parser.add_argument('--limit_pages', type=int, default=1, help='Số lượng trang muốn cào mỗi lần chạy')
    parser.add_argument('--start_page', type=int, help='Trang bắt đầu cào (ghi đè file trạng thái nếu truyền, chỉ dùng khi truyền --category)')
    args = parser.parse_args()
    
    CATEGORY_ID = args.category
    CATEGORY_NAME = args.category_name
    LIMIT_PAGES = args.limit_pages
    START_PAGE_OVERRIDE = args.start_page
    
    if CATEGORY_ID is not None:
        if not CATEGORY_NAME:
            CATEGORY_NAME = DEFAULT_TIKI_CATEGORIES.get(CATEGORY_ID, f"Category_{CATEGORY_ID}")
        try:
            run_single_category(CATEGORY_ID, CATEGORY_NAME, LIMIT_PAGES, START_PAGE_OVERRIDE)
        except Exception as e:
            print(f"❌ Lỗi trong quá trình cào dữ liệu danh mục {CATEGORY_ID}: {e}")
    else:
        try:
            run_sequential_crawler(LIMIT_PAGES)
        except Exception as e:
            print(f"❌ Lỗi trong quá trình cào tuần tự: {e}")

if __name__ == "__main__":
    main()