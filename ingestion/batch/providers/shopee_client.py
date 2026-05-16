from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import boto3
from dotenv import load_dotenv
from botocore.client import Config
import json
import time
import os
import io
import random
from urllib.parse import quote

load_dotenv()


def _get_int_env(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _get_float_env(name, default):
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


class ShopeeApiClient:
    def __init__(self):
        # 1. Cấu hình Selenium hướng về Browserless để lấy cookie Shopee
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--window-size=1440,900")
        self.options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

        self.browserless_url = os.getenv("BROWSERLESS_URL", "http://browserless_chrome:3000/webdriver")

        # 2. Cấu hình Hive Path & MinIO
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.provider = "shopee"
        self.hive_path = f"provider={self.provider}/date={self.today}"

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

        # 3. Khởi tạo browser session dùng cho các request API nội bộ của Shopee
        self.user_agent = None
        self.driver = None
        self.current_search_keyword = None
        self.page_size = min(_get_int_env("SHOPEE_PAGE_SIZE", 60), 60)
        self.review_limit = min(_get_int_env("SHOPEE_REVIEW_LIMIT", 50), 50)
        self.review_pages = max(_get_int_env("SHOPEE_REVIEW_PAGES", 1), 1)
        self.max_review_products = _get_int_env("SHOPEE_MAX_REVIEW_PRODUCTS", 10)
        self.min_delay = _get_float_env("SHOPEE_MIN_DELAY_SECONDS", 3.0)
        self.max_delay = _get_float_env("SHOPEE_MAX_DELAY_SECONDS", 8.0)
        self.max_retries = max(_get_int_env("SHOPEE_MAX_RETRIES", 3), 1)
        self.backoff_base = _get_float_env("SHOPEE_BACKOFF_BASE_SECONDS", 5.0)
        self.home_wait_seconds = _get_float_env("SHOPEE_HOME_WAIT_SECONDS", 8.0)
        self.search_wait_seconds = _get_float_env("SHOPEE_SEARCH_WAIT_SECONDS", 8.0)
        self._refresh_session()

    def _init_driver(self):
        try:
            return webdriver.Remote(
                command_executor=self.browserless_url,
                options=self.options
            )
        except Exception as e:
            print(f"❌ Không thể kết nối tới Browserless tại {self.browserless_url}: {e}")
            raise e

    def _refresh_session(self):
        """Dùng Selenium mở Shopee để vượt qua tầng kiểm tra ban đầu và lấy Cookie"""
        print("[System] Đang khởi tạo phiên làm việc (Session)...")
        self.close()
        self.driver = self._init_driver()
        try:
            self.driver.set_script_timeout(30)
            self.driver.get("https://shopee.vn")
            time.sleep(self.home_wait_seconds)
            self.user_agent = self.driver.execute_script("return navigator.userAgent")
            self.current_search_keyword = None
            print("[System] Session đã giả lập xong dấu vân tay trình duyệt!")
        except Exception:
            self.close()
            raise

    def close(self):
        if not self.driver:
            return

        try:
            self.driver.quit()
        except Exception:
            pass
        finally:
            self.driver = None

    def _sleep(self, min_delay=None, max_delay=None):
        min_delay = self.min_delay if min_delay is None else min_delay
        max_delay = self.max_delay if max_delay is None else max_delay

        if max_delay < min_delay:
            max_delay = min_delay

        time.sleep(random.uniform(min_delay, max_delay))

    def _ensure_not_traffic_blocked(self):
        current_url = self.driver.current_url
        if "/verify/traffic/error" not in current_url:
            return

        body = self.driver.find_element("tag name", "body").text
        message = body.replace("\n", " ")[:300]
        raise RuntimeError(
            "Shopee đang chặn phiên Browserless/headless: "
            f"{current_url}. Nội dung trang: {message}"
        )

    def _load_search_page(self, keyword):
        if self.current_search_keyword == keyword:
            return

        encoded_keyword = quote(keyword)
        self.driver.get(f"https://shopee.vn/search?keyword={encoded_keyword}")
        time.sleep(self.search_wait_seconds)
        self._ensure_not_traffic_blocked()
        self.current_search_keyword = keyword

    def _safe_browser_headers(self, headers):
        unsafe_headers = {"cookie", "host", "referer", "user-agent", "origin"}
        return {
            key: value
            for key, value in headers.items()
            if key.lower() not in unsafe_headers and value is not None
        }

    def _browser_fetch_json(self, url, headers, timeout):
        script = """
            const url = arguments[0];
            const headers = arguments[1];
            const timeoutMs = arguments[2] * 1000;
            const done = arguments[arguments.length - 1];
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), timeoutMs);

            fetch(url, {
                method: "GET",
                credentials: "include",
                headers,
                signal: controller.signal,
            })
                .then(async (response) => {
                    const text = await response.text();
                    clearTimeout(timer);

                    let data = {};
                    try {
                        data = text ? JSON.parse(text) : {};
                    } catch (error) {
                        data = {};
                    }

                    done({
                        ok: response.ok,
                        status: response.status,
                        data,
                        text: text.slice(0, 500),
                    });
                })
                .catch((error) => {
                    clearTimeout(timer);
                    done({
                        ok: false,
                        status: 0,
                        data: {},
                        error: String(error),
                    });
                });
        """
        self.driver.set_script_timeout(timeout + 5)
        return self.driver.execute_async_script(
            script,
            url,
            self._safe_browser_headers(headers),
            timeout,
        )

    def _request_json(self, url, headers, timeout=15):
        for attempt in range(1, self.max_retries + 1):
            try:
                result = self._browser_fetch_json(url, headers, timeout)
                status = result.get("status", 0)

                if result.get("ok") and status == 200:
                    return result.get("data") or {}

                print(f"    [Warn] Browser API trả về {status} ở lần thử {attempt}/{self.max_retries}")
                if result.get("error"):
                    print(f"    [Warn] Chi tiết lỗi browser fetch: {result['error']}")

                if status in (401, 403, 429) and attempt == 1:
                    self._refresh_session()

            except Exception as e:
                print(f"    [Warn] Lỗi browser request lần {attempt}/{self.max_retries}: {e}")
                if attempt == 1:
                    self._refresh_session()

            if attempt < self.max_retries:
                backoff = self.backoff_base * attempt
                time.sleep(backoff + random.uniform(0, self.backoff_base))

        return {}

    def upload_data_to_minio(self, data, category_name, file_name):
        object_name = f"{self.hive_path}/category={category_name}/{file_name}"

        try:
            json_str = json.dumps(data, ensure_ascii=False, indent=4)
            data_stream = io.BytesIO(json_str.encode("utf-8"))

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_name,
                Body=data_stream,
                ContentType="application/json"
            )
            print(f"    [MinIO-Stream] ✔ Đã upload: {object_name}")
        except Exception as e:
            print(f"    [MinIO-Stream] ✘ Lỗi upload: {e}")

    def get_products(self, keyword, page=0, limit=None):
        limit = min(limit or self.page_size, 60)
        newest = page * limit
        encoded_keyword = quote(keyword)
        url = f"https://shopee.vn/api/v4/search/search_items?by=relevancy&keyword={encoded_keyword}&limit={limit}&newest={newest}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2"
        self._load_search_page(keyword)

        # Tạo bộ Headers khớp với trình duyệt thật
        headers = {
            "User-Agent": self.user_agent,
            "Referer": f"https://shopee.vn/search?keyword={encoded_keyword}",
            "X-Shopee-Language": "vi",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"
        }

        data = self._request_json(url, headers, timeout=15)
        items = data.get('items', []) or []
        return [i.get('item_basic') for i in items if i.get('item_basic')]

    def get_reviews(self, item_id, shop_id, limit=None, offset=0):
        if not item_id or not shop_id:
            return []

        limit = min(limit or self.review_limit, 50)
        url = f"https://shopee.vn/api/v2/item/get_ratings?itemid={item_id}&shopid={shop_id}&limit={limit}&offset={offset}"
        headers = {"User-Agent": self.user_agent, "Referer": "https://shopee.vn/"}

        data = self._request_json(url, headers, timeout=10)
        return data.get('data', {}).get('ratings', []) or []

    def get_all_reviews(self, item_id, shop_id, max_pages=None):
        max_pages = max_pages or self.review_pages
        reviews = []

        for review_page in range(max_pages):
            offset = review_page * self.review_limit
            page_reviews = self.get_reviews(item_id, shop_id, limit=self.review_limit, offset=offset)

            if not page_reviews:
                break

            reviews.extend(page_reviews)

            if len(page_reviews) < self.review_limit:
                break

            self._sleep()

        return reviews

    def _products_for_reviews(self, products, review_products_limit):
        if review_products_limit is None:
            review_products_limit = self.max_review_products

        if review_products_limit <= 0:
            return products

        return products[:review_products_limit]

    def crawl_all(self, keyword, start_page=0, end_page=0, review_products_limit=None, review_pages=None):
        try:
            for page in range(start_page, end_page + 1):
                print(f"\n--- Đang cào trang {page} ---")
                products = self.get_products(keyword, page)

                if not products:
                    print("    ⚠ Không có dữ liệu. Shopee có thể đang chặn session hoặc API đã đổi.")
                    continue

                ts = int(time.time())
                p_file = f"products_pg{page}_{ts}.json"
                self.upload_data_to_minio(products, "products", p_file)

                review_products = self._products_for_reviews(products, review_products_limit)
                print(f"    [Reviews] Sẽ lấy review cho {len(review_products)}/{len(products)} sản phẩm")

                for p in review_products:
                    item_id, shop_id = p.get('itemid'), p.get('shopid')
                    reviews = self.get_all_reviews(item_id, shop_id, max_pages=review_pages)

                    if reviews:
                        r_file = f"reviews_item_{item_id}_{ts}.json"
                        self.upload_data_to_minio(reviews, "reviews", r_file)

                    self._sleep()

                self._sleep()
        finally:
            self.close()
