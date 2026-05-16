from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import boto3
from dotenv import load_dotenv
from botocore.client import Config
import json
import time
import os
import io  # Sử dụng BytesIO để stream dữ liệu từ RAM lên thẳng Object Storage

load_dotenv()

class TikiApiClient:
    def __init__(self):
        # 1. Cấu hình Selenium hướng về Browserless
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        
        self.browserless_url = os.getenv("BROWSERLESS_URL", "http://browserless_chrome:3000/webdriver")
        
        # 2. Định nghĩa các biến Partition (Hive Style)
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.provider = "tiki"
        self.hive_path = f"provider={self.provider}/date={self.today}"
        
        # Loại bỏ cấu hình local_root (không cần dùng ổ đĩa nữa)
        
        # 3. Cấu hình MinIO
        endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        self.bucket_name = os.getenv("MINIO_BUCKET_NAME", "bronze-lakehouse")
        
        self.s3_client = boto3.client('s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )

    def _init_driver(self):
        try:
            return webdriver.Remote(
                command_executor=self.browserless_url,
                options=self.options
            )
        except Exception as e:
            print(f"❌ Không thể kết nối tới Browserless tại {self.browserless_url}: {e}")
            raise e

    def _fetch_json(self, url, wait_seconds=3, error_context="request"):
        driver = self._init_driver()
        try:
            driver.get(url)
            time.sleep(wait_seconds)
            content = driver.find_element("tag name", "body").text
            return json.loads(content)
        except Exception as e:
            print(f"Lỗi {error_context}: {e}")
            return None
        finally:
            driver.quit()

    def upload_data_to_minio(self, data, category_name, file_name):
        """
        Thay đổi cốt lõi: Chuyển dữ liệu Python thành chuỗi JSON dạng byte ngay trên RAM
        và sử dụng put_object để đẩy thẳng lên MinIO không thông qua ổ đĩa.
        """
        object_name = f"{self.hive_path}/category={category_name}/{file_name}"
        
        try:
            # Mã hóa dữ liệu trực tiếp trong bộ nhớ RAM
            json_str = json.dumps(data, ensure_ascii=False, indent=4)
            json_bytes = json_str.encode('utf-8')
            
            # Tạo một luồng byte trong bộ nhớ (In-memory stream)
            data_stream = io.BytesIO(json_bytes)
            
            # Đẩy dữ liệu lên MinIO
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_name,
                Body=data_stream,
                ContentType='application/json'
            )
            print(f"    [MinIO-Stream] ✔ Đã upload thẳng lên RAM: {object_name}")
        except Exception as e:
            print(f"    [MinIO-Stream] ✘ Lỗi upload trực tiếp: {e}")

    def get_products(self, category_id=1846, page=1, limit=40):
        url = f"https://tiki.vn/api/v2/products?category={category_id}&page={page}&limit={limit}"
        data = self._fetch_json(url, wait_seconds=5, error_context=f"lấy sản phẩm trang {page}")
        return (data or {}).get('data', [])

    def get_product_detail(self, product_id):
        url = f"https://tiki.vn/api/v2/products/{product_id}"
        return self._fetch_json(url, wait_seconds=3, error_context=f"lấy chi tiết SP {product_id}")

    def get_product_reviews(self, product_id, limit=20):
        url = f"https://tiki.vn/api/v2/reviews?product_id={product_id}&limit={limit}"
        data = self._fetch_json(url, wait_seconds=3, error_context=f"lấy review cho SP {product_id}")
        return (data or {}).get('data', [])

    def build_seller_info(self, product, product_detail):
        """
        Lấy shop URL từ product detail. Không dùng /api/v2/stores/{seller_id}
        vì endpoint đó cần store_id và dễ trả URL shop cũ/sai.
        """
        current_seller = (product_detail or {}).get("current_seller") or {}
        seller_id = current_seller.get("id") or product.get("seller_id")
        store_id = current_seller.get("store_id")
        shop_url = current_seller.get("link")

        if not seller_id or not shop_url:
            return None

        return {
            "seller_id": seller_id,
            "seller_name": current_seller.get("name") or product.get("seller_name"),
            "seller_sku": current_seller.get("sku"),
            "seller_logo": current_seller.get("logo"),
            "store_id": store_id,
            "shop_url": shop_url,
            "seller_product_id": current_seller.get("product_id") or product.get("seller_product_id"),
            "current_price": current_seller.get("price") or product.get("price"),
            "is_best_store": current_seller.get("is_best_store"),
            "source_product_id": product.get("id"),
            "source_product_name": product.get("name"),
        }

    def crawl_all(self, category_id, start_page=1, end_page=1):
        # ĐÃ XÓA TOÀN BỘ CÁC LỆNH os.makedirs GÂY TẠO THƯ MỤC RÁC LOCAL
        seen_sellers = set()

        for page in range(start_page, end_page + 1):
            print(f"\n--- Đang xử lý TRANG {page} ---")
            products = self.get_products(category_id, page=page)
            
            if not products: continue

            ts = int(time.time())
            prod_file = f"batch_pg{page}_{ts}.json"
            
            # Đẩy trực tiếp danh sách sản phẩm lên MinIO
            self.upload_data_to_minio(products, "products", prod_file)

            # Duyệt qua từng sản phẩm từ API trả về để bóc tách thông tin đi kèm
            for p in products:
                p_id = p.get('id')
                
                if p_id:
                    # # 1. Product detail chứa current_seller.link là URL shop đúng.
                    # product_detail = self.get_product_detail(p_id)
                    # if product_detail:
                    #     detail_file = f"detail_sp_{p_id}_{ts}.json"
                    #     self.upload_data_to_minio(product_detail, "product_details", detail_file)

                    #     seller_info = self.build_seller_info(p, product_detail)
                    #     if seller_info:
                    #         seller_key = seller_info.get("store_id") or seller_info.get("seller_id")
                    #         if seller_key not in seen_sellers:
                    #             seller_file = f"seller_{seller_info['seller_id']}_{ts}.json"
                    #             self.upload_data_to_minio(seller_info, "sellers", seller_file)
                    #             seen_sellers.add(seller_key)

                    # 2. Xử lý Reviews trực tiếp lên MinIO
                    reviews = self.get_product_reviews(p_id)
                    if reviews:
                        rev_file = f"reviews_sp_{p_id}_{ts}.json"
                        self.upload_data_to_minio(reviews, "reviews", rev_file)
                    time.sleep(1)
