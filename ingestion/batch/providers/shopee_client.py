from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from curl_cffi import requests  # Thay thế requests thường bằng curl_cffi
from datetime import datetime
import boto3
from botocore.client import Config
import json
import time
import os

class ShopeeApiClient:
    def __init__(self):
        # 1. Cấu hình Selenium lấy Cookie
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("window-size=1440,900")
        self.options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        
        # 2. Cấu hình Hive Path & MinIO (Giữ nguyên của bạn)
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.provider = "shopee"
        self.hive_path = f"provider={self.provider}/date={self.today}"
        self.local_root = "raw_data"
        
        self.s3_client = boto3.client('s3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='o8G5wmNfm5rObtrDtQEc',      
            aws_secret_access_key='1mXPbGQDnpOPwLMOXVdraR3XuhMLX7OVjGdWXspj', 
            config=Config(signature_version='s3v4')
        )
        self.bucket_name = "bronze-lakehouse"

        # 3. Khởi tạo Session với curl_cffi (Chỉ định giả lập Chrome bản mới)
        self.session = requests.Session(impersonate="chrome")
        self._refresh_session()

    def _refresh_session(self):
        """Dùng Selenium mở Shopee để vượt qua tầng kiểm tra ban đầu và lấy Cookie"""
        print("[System] Đang khởi tạo phiên làm việc (Session)...")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
        driver.get("https://shopee.vn")
        time.sleep(6) # Để trang chủ tải hoàn chỉnh
        
        # Đồng bộ Cookie sang curl_cffi session
        for cookie in driver.get_cookies():
            self.session.cookies.set(cookie['name'], cookie['value'])
        
        self.user_agent = driver.execute_script("return navigator.userAgent")
        driver.quit()
        print("[System] Session đã giả lập xong dấu vân tay trình duyệt!")

    def upload_to_minio(self, local_path, category_name):
        file_name = os.path.basename(local_path)
        object_name = f"{self.hive_path}/category={category_name}/{file_name}"
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, object_name)
            print(f"    [MinIO] ✔ Đã đẩy: {object_name}")
        except Exception as e:
            print(f"    [MinIO] ✘ Lỗi upload: {e}")

    def get_products(self, keyword, page=0):
        newest = page * 60
        url = f"https://shopee.vn/api/v4/search/search_items?by=relevancy&keyword={keyword}&limit=60&newest={newest}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2"
        
        # Tạo bộ Headers khớp với trình duyệt thật
        headers = {
            "User-Agent": self.user_agent,
            "Referer": f"https://shopee.vn/search?keyword={keyword}",
            "X-Shopee-Language": "vi",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"
        }

        try:
            # Gửi request đi với tư cách là một trình duyệt Chrome xịn
            response = self.session.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', []) or []
                return [i.get('item_basic') for i in items if i.get('item_basic')]
            else:
                print(f"    [Error] API trả về lỗi {response.status_code}")
                return []
        except Exception as e:
            print(f"    [Error] Lỗi kết nối API: {e}")
            return []

    def get_reviews(self, item_id, shop_id, limit=20):
        url = f"https://shopee.vn/api/v2/item/get_ratings?itemid={item_id}&shopid={shop_id}&limit={limit}&offset=0"
        headers = {"User-Agent": self.user_agent, "Referer": "https://shopee.vn/"}
        try:
            res = self.session.get(url, headers=headers, timeout=10)
            return res.json().get('data', {}).get('ratings', [])
        except:
            return []

    def crawl_all(self, keyword, start_page=0, end_page=0):
        prod_dir = f"{self.local_root}/{self.hive_path}/category=products"
        rev_dir = f"{self.local_root}/{self.hive_path}/category=reviews"
        os.makedirs(prod_dir, exist_ok=True)
        os.makedirs(rev_dir, exist_ok=True)

        for page in range(start_page, end_page + 1):
            print(f"\n--- Đang cào trang {page} ---")
            products = self.get_products(keyword, page)
            
            if not products:
                print("    ⚠ Không có dữ liệu. Vui lòng kiểm tra lại từ khóa hoặc tăng time.sleep.")
                continue

            # Lưu và đẩy Products lên MinIO
            ts = int(time.time())
            p_file = f"products_pg{page}_{ts}.json"
            p_path = f"{prod_dir}/{p_file}"
            with open(p_path, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=4)
            self.upload_to_minio(p_path, "products")

            # Lưu và đẩy Reviews lên MinIO
            for p in products[:3]: # Lấy trước 3 sản phẩm để kiểm tra pipeline
                item_id, shop_id = p.get('itemid'), p.get('shopid')
                reviews = self.get_reviews(item_id, shop_id)
                if reviews:
                    r_file = f"rev_{item_id}.json"
                    r_path = f"{rev_dir}/{r_file}"
                    with open(r_path, 'w', encoding='utf-8') as f:
                        json.dump(reviews, f, ensure_ascii=False, indent=4)
                    self.upload_to_minio(r_path, "reviews")
                time.sleep(3)