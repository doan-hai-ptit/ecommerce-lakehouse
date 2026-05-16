from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import boto3
from dotenv import load_dotenv
from botocore.client import Config
import json
import time
import os

load_dotenv()

class TikiApiClient:
    def __init__(self):
        # 1. Cấu hình Selenium
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        
        # 2. Định nghĩa các biến Partition (Hive Style)
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.provider = "tiki"
        
        # Cấu trúc folder chuẩn: provider=tiki/date=2026-05-15
        self.hive_path = f"provider={self.provider}/date={self.today}"
        self.local_root = "raw_data"
        endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        self.bucket_name = os.getenv("MINIO_BUCKET_NAME", "bronze-lakehouse")
        # 3. Cấu hình MinIO
        self.s3_client = boto3.client('s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
        self.bucket_name = "bronze-lakehouse"

    def _init_driver(self):
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)

    def upload_to_minio(self, local_path, category_name):
        """
        Đẩy file lên MinIO theo chuẩn Hive Partitioning:
        bronze-lakehouse/provider=tiki/date=2026-05-15/category=products/file.json
        """
        file_name = os.path.basename(local_path)
        # Tạo đường dẫn object chuẩn Hive
        object_name = f"{self.hive_path}/category={category_name}/{file_name}"
        
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, object_name)
            print(f"    [MinIO] ✔ Đã đẩy: {object_name}")
        except Exception as e:
            print(f"    [MinIO] ✘ Lỗi upload: {e}")

    def get_products(self, category_id=1846, page=1, limit=40):
        url = f"https://tiki.vn/api/v2/products?category={category_id}&page={page}&limit={limit}"
        driver = self._init_driver()
        try:
            driver.get(url)
            time.sleep(5)
            content = driver.find_element("tag name", "body").text
            return json.loads(content).get('data', [])
        except Exception as e:
            print(f"Lỗi lấy sản phẩm trang {page}: {e}")
            return []
        finally:
            driver.quit()

    def get_product_reviews(self, product_id, limit=20):
        url = f"https://tiki.vn/api/v2/reviews?product_id={product_id}&limit={limit}"
        driver = self._init_driver()
        try:
            driver.get(url)
            time.sleep(3)
            content = driver.find_element("tag name", "body").text
            return json.loads(content).get('data', [])
        except Exception as e:
            print(f"Lỗi lấy review cho SP {product_id}: {e}")
            return []
        finally:
            driver.quit()

    def crawl_all(self, category_id, start_page=1, end_page=1):
        prod_dir = f"{self.local_root}/{self.hive_path}/category=products"
        rev_dir = f"{self.local_root}/{self.hive_path}/category=reviews"
        seller_dir = f"{self.local_root}/{self.hive_path}/category=sellers"
        
        os.makedirs(prod_dir, exist_ok=True)
        os.makedirs(rev_dir, exist_ok=True)
        os.makedirs(seller_dir, exist_ok=True)

        # Khởi tạo set để theo dõi seller đã cào, tránh spam request trùng một shop
        crawled_sellers = set()

        for page in range(start_page, end_page + 1):
            print(f"\n--- Đang xử lý TRANG {page} ---")
            products = self.get_products(category_id, page=page)
            
            if not products: continue

            ts = int(time.time())
            prod_file = f"batch_pg{page}_{ts}.json"
            prod_path = f"{prod_dir}/{prod_file}"
            
            with open(prod_path, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=4)
            
            self.upload_to_minio(prod_path, "products")

            # Duyệt qua từng sản phẩm từ API trả về
            for p in products:
                p_id = p.get('id')
                
                # 1. Xử lý bốc tách Reviews
                if p_id:
                    reviews = self.get_product_reviews(p_id)
                    if reviews:
                        rev_file = f"reviews_sp_{p_id}_{ts}.json"
                        rev_path = f"{rev_dir}/{rev_file}"
                        with open(rev_path, 'w', encoding='utf-8') as f:
                            json.dump(reviews, f, ensure_ascii=False, indent=4)
                        self.upload_to_minio(rev_path, "reviews")
                    time.sleep(1)

                # 2. Xử lý bốc tách Seller Info (Cập nhật theo đúng file JSON của bạn)
                # seller_id = p.get('seller_id') # Lấy trực tiếp từ key 'seller_id' ngang hàng với name, price

                # if seller_id and seller_id not in crawled_sellers:
                #     print(f"  -> Phát hiện đối tác mới. Đang cào Seller ID: {seller_id}")
                #     seller_info = self.get_seller_info(seller_id)
                    
                #     if seller_info:
                #         seller_file = f"seller_{seller_id}_{ts}.json"
                #         seller_path = f"{seller_dir}/{seller_file}"
                        
                #         with open(seller_path, 'w', encoding='utf-8') as f:
                #             json.dump(seller_info, f, ensure_ascii=False, indent=4)
                        
                #         self.upload_to_minio(seller_path, "sellers")
                        
                #         # Đưa vào hàng chờ đã xử lý
                #         crawled_sellers.add(seller_id)
                #     time.sleep(1)
    def get_seller_info(self, seller_id):
        """
        Cào thông tin chi tiết của một Nhà bán hàng (Seller) dựa trên seller_id
        """
        url = f"https://tiki.vn/api/v2/stores/{seller_id}"
        driver = self._init_driver()
        try:
            driver.get(url)
            time.sleep(3)  # Chờ trình duyệt load nội dung JSON
            content = driver.find_element("tag name", "body").text
            
            # API trả về dữ liệu thô của store, ta parse trực tiếp thành dict
            return json.loads(content)
        except Exception as e:
            print(f"Lỗi lấy thông tin nhà bán {seller_id}: {e}")
            return None
        finally:
            driver.quit()
