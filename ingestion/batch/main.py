from providers.tiki_client import TikiApiClient
from providers.shopee_client import ShopeeApiClient
import os

def main():
    print("=== Hệ thống Ingestion Batch - Project: ecommerce-lakehouse ===")
    client = TikiApiClient()
    
    # Ví dụ: Hải muốn cào tiếp từ trang 4 đến trang 6
    START = 11
    END = 11
    CATEGORY_ID = 1846 # Laptop
    
    print(f"\n[Bắt đầu Ingestion] Ngày: {client.today} | Từ trang {START} đến {END}")
    
    client.crawl_all(category_id=CATEGORY_ID, start_page=START, end_page=END)
    
    print("\n[Hoàn tất] Dữ liệu đã nằm trong đúng folder partition ngay=...")
    # client = ShopeeApiClient()
    
    # # Shopee bắt đầu tính từ trang 0 (Trang 1 trên giao diện)
    # START_PAGE = 0 
    # END_PAGE = 0
    # KEYWORD = "laptop dell"  # Nhập từ khóa bạn muốn cào thay cho ID danh mục
    
    # print(f"\n[Bắt đầu Ingestion] Ngày: {client.today} | Từ trang {START_PAGE} đến {END_PAGE}")
    
    # client.crawl_all(keyword=KEYWORD, start_page=START_PAGE, end_page=END_PAGE)
    
    # print("\n[Hoàn tất] Tiến trình cào dữ liệu Shopee hoàn tất!")

if __name__ == "__main__":
    main()
