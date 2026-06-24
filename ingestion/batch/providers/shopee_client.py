from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime
import boto3
from dotenv import load_dotenv
from botocore.client import Config
import json
import time
import os
import io
import random
import re
import requests
from urllib.parse import quote, urljoin

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
        # 1. Cấu hình Selenium fallback để scrape HTML khi API không trả dữ liệu.
        self.options = Options()
        self.driver_mode = os.getenv("SHOPEE_DRIVER", "browserless").lower()
        self.headless = os.getenv("SHOPEE_HEADLESS", "true").lower() in ("1", "true", "yes")

        if self.headless:
            self.options.add_argument("--headless=new")

        self.options.page_load_strategy = os.getenv("SHOPEE_PAGE_LOAD_STRATEGY", "none")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--window-size=1440,900")
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--disable-infobars")
        self.options.add_argument("--lang=vi-VN")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option("useAutomationExtension", False)
        self.options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        user_agent = os.getenv("SHOPEE_USER_AGENT")
        if user_agent:
            self.options.add_argument(f"user-agent={user_agent}")

        self.user_data_dir = os.getenv("SHOPEE_USER_DATA_DIR")
        if self.user_data_dir:
            self.options.add_argument(f"--user-data-dir={self.user_data_dir}")

        self.browser_binary = os.getenv("SHOPEE_BROWSER_BINARY")
        if self.browser_binary:
            self.options.binary_location = self.browser_binary

        self.browserless_url = os.getenv("BROWSERLESS_URL", "http://browserless_chrome:3000/webdriver")

        # 2. Cấu hình Hive Path & MinIO.
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.provider = "shopee"
        self.hive_path = f"provider={self.provider}/date={self.today}"

        endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        self.bucket_name = os.getenv("MINIO_BUCKET_NAME", "bronze-lakehouse")

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )

        # 3. Tham số crawl HTML.
        self.user_agent = None
        self.driver = None
        self.current_search_key = None
        self.page_size = min(_get_int_env("SHOPEE_PAGE_SIZE", 60), 60)
        self.review_limit = min(_get_int_env("SHOPEE_REVIEW_LIMIT", 50), 50)
        self.review_pages = max(_get_int_env("SHOPEE_REVIEW_PAGES", 1), 1)
        self.max_review_products = _get_int_env("SHOPEE_MAX_REVIEW_PRODUCTS", 10)
        self.min_delay = _get_float_env("SHOPEE_MIN_DELAY_SECONDS", 3.0)
        self.max_delay = _get_float_env("SHOPEE_MAX_DELAY_SECONDS", 8.0)
        self.home_wait_seconds = _get_float_env("SHOPEE_HOME_WAIT_SECONDS", 1.0)
        self.search_wait_seconds = _get_float_env("SHOPEE_SEARCH_WAIT_SECONDS", 8.0)
        self.verify_wait_seconds = _get_float_env("SHOPEE_VERIFY_WAIT_SECONDS", 0)
        self.manual_verify = os.getenv("SHOPEE_MANUAL_VERIFY", "false").lower() in ("1", "true", "yes")
        self.sort_by = os.getenv("SHOPEE_SORT_BY", "sales")
        self.scroll_rounds = max(_get_int_env("SHOPEE_SCROLL_ROUNDS", 8), 1)
        self.scroll_pause_seconds = _get_float_env("SHOPEE_SCROLL_PAUSE_SECONDS", 1.2)
        self.card_wait_seconds = _get_float_env("SHOPEE_CARD_WAIT_SECONDS", 20.0)
        self.detail_wait_seconds = _get_float_env("SHOPEE_DETAIL_WAIT_SECONDS", 10.0)
        self.page_load_timeout_seconds = _get_float_env("SHOPEE_PAGE_LOAD_TIMEOUT_SECONDS", 12.0)
        self.browser_restart_retries = max(_get_int_env("SHOPEE_BROWSER_RESTART_RETRIES", 1), 0)
        self.browser_api_timeout_seconds = _get_float_env("SHOPEE_BROWSER_API_TIMEOUT_SECONDS", 25.0)
        self.fetch_mode = os.getenv("SHOPEE_FETCH_MODE", "api_then_html").lower()
        self.api_base_url = os.getenv("SHOPEE_API_BASE_URL", "https://shopee.vn").rstrip("/")
        self.api_timeout_seconds = _get_float_env("SHOPEE_API_TIMEOUT_SECONDS", 15.0)
        self.api_max_retries = max(_get_int_env("SHOPEE_API_MAX_RETRIES", 2), 0)
        self.api_retry_delay_seconds = _get_float_env("SHOPEE_API_RETRY_DELAY_SECONDS", 2.0)
        self.api_session = requests.Session()
        self._configure_api_session()

        valid_fetch_modes = ("api", "html", "api_then_html", "browser_api", "browser_api_then_html")
        if self.fetch_mode not in valid_fetch_modes:
            print(f"[Warn] SHOPEE_FETCH_MODE={self.fetch_mode} không hợp lệ, dùng api_then_html.")
            self.fetch_mode = "api_then_html"

    def _configure_api_session(self):
        user_agent = os.getenv(
            "SHOPEE_USER_AGENT",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
        )
        self.api_session.headers.update(
            {
                "accept": "application/json",
                "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "referer": f"{self.api_base_url}/",
                "user-agent": user_agent,
                "x-api-source": "pc",
                "x-requested-with": "XMLHttpRequest",
            }
        )

        cookie_header = os.getenv("SHOPEE_COOKIE") or os.getenv("SHOPEE_COOKIE_HEADER")
        if cookie_header:
            self.api_session.headers["cookie"] = cookie_header
            print("[System] Đã nạp SHOPEE_COOKIE cho Shopee API session.")

    def _build_undetected_options(self):
        import undetected_chromedriver as uc

        options = uc.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1440,900")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("--lang=vi-VN")

        if self.user_data_dir:
            options.add_argument(f"--user-data-dir={self.user_data_dir}")
        if self.browser_binary:
            options.binary_location = self.browser_binary

        user_agent = os.getenv("SHOPEE_USER_AGENT")
        if user_agent:
            options.add_argument(f"user-agent={user_agent}")

        return options

    def _init_driver(self):
        try:
            if self.driver_mode == "undetected":
                import undetected_chromedriver as uc

                version_main = os.getenv("SHOPEE_CHROME_VERSION_MAIN")
                return uc.Chrome(
                    options=self._build_undetected_options(),
                    browser_executable_path=self.browser_binary,
                    version_main=int(version_main) if version_main else None,
                    use_subprocess=True,
                )

            if self.driver_mode == "local":
                return webdriver.Chrome(options=self.options)

            return webdriver.Remote(
                command_executor=self.browserless_url,
                options=self.options,
            )
        except Exception as e:
            if self.driver_mode == "undetected":
                target = "undetected Chrome local"
            elif self.driver_mode == "local":
                target = "Chrome local"
            else:
                target = f"Browserless tại {self.browserless_url}"
            print(f"❌ Không thể kết nối tới {target}: {e}")
            raise

    def _install_stealth_script(self):
        stealth_source = """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = window.chrome || { runtime: {} };

            if (!window.__shopeeCaptureInstalled) {
                window.__shopeeCaptureInstalled = true;
                window.__shopeeCapturedResponses = [];

                const shouldCaptureShopeeApi = (url) => String(url || '').includes('/api/v4/search/search_items');
                const pushShopeeApiResponse = (url, status, bodyText) => {
                    try {
                        window.__shopeeCapturedResponses.push({
                            url: String(url || ''),
                            status: status || null,
                            body: JSON.parse(bodyText),
                            capturedAt: Date.now(),
                        });
                    } catch (error) {
                        window.__shopeeCapturedResponses.push({
                            url: String(url || ''),
                            status: status || null,
                            rawText: String(bodyText || '').slice(0, 2000),
                            capturedAt: Date.now(),
                        });
                    }
                };

                const originalFetch = window.fetch;
                if (typeof originalFetch === 'function') {
                    window.fetch = async function(...args) {
                        const response = await originalFetch.apply(this, args);
                        const request = args[0];
                        const url = typeof request === 'string' ? request : (request && request.url);
                        if (shouldCaptureShopeeApi(url || response.url)) {
                            response.clone().text().then((bodyText) => {
                                pushShopeeApiResponse(url || response.url, response.status, bodyText);
                            }).catch(() => {});
                        }
                        return response;
                    };
                }

                const originalOpen = window.XMLHttpRequest && window.XMLHttpRequest.prototype.open;
                const originalSend = window.XMLHttpRequest && window.XMLHttpRequest.prototype.send;
                if (originalOpen && originalSend) {
                    window.XMLHttpRequest.prototype.open = function(method, url, ...rest) {
                        this.__shopeeCaptureUrl = url;
                        return originalOpen.call(this, method, url, ...rest);
                    };
                    window.XMLHttpRequest.prototype.send = function(...args) {
                        this.addEventListener('load', function() {
                            if (shouldCaptureShopeeApi(this.__shopeeCaptureUrl || this.responseURL)) {
                                pushShopeeApiResponse(
                                    this.__shopeeCaptureUrl || this.responseURL,
                                    this.status,
                                    this.responseText || ''
                                );
                            }
                        });
                        return originalSend.apply(this, args);
                    };
                }
            }

            const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
            if (originalQuery) {
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications'
                        ? Promise.resolve({ state: Notification.permission })
                        : originalQuery(parameters)
                );
            }
        """

        try:
            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": stealth_source},
            )
            self.driver.execute_script(stealth_source)
            print("[System] Đã cài stealth script cho Selenium.")
        except Exception as e:
            print(f"[Warn] Không cài được stealth script qua CDP: {e}")

    def _refresh_session(self):
        """Khởi tạo browser session, không mở trang login/home của Shopee."""
        print("[System] Đang khởi tạo Selenium browser session...")
        self.close()
        self.driver = self._init_driver()
        try:
            self.driver.set_script_timeout(30)
            self.driver.set_page_load_timeout(self.page_load_timeout_seconds)
            self._install_stealth_script()
            try:
                self.driver.execute_cdp_cmd("Network.enable", {})
            except Exception as e:
                print(f"[Warn] Không bật được CDP Network logging: {e}")
            self.driver.get("about:blank")
            time.sleep(self.home_wait_seconds)
            self.user_agent = self.driver.execute_script("return navigator.userAgent")
            self.current_search_key = None
            print("[System] Selenium session đã sẵn sàng.")
        except Exception:
            self.close()
            raise

    def _ensure_driver(self):
        if self.driver:
            return

        self._refresh_session()

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

        if self.manual_verify:
            print(
                "    [Verify] Shopee đang hiện trang verify/captcha. "
                "Hãy xử lý trên cửa sổ browser, rồi quay lại terminal và nhấn Enter..."
            )
            input()
            current_url = self.driver.current_url
            if "/verify/traffic/error" not in current_url:
                print("    [Verify] Đã thoát trang verify, tiếp tục crawl.")
                return

        if self.verify_wait_seconds > 0:
            print(
                "    [Verify] Shopee đang hiện trang verify. "
                f"Bạn có {int(self.verify_wait_seconds)} giây để xử lý trên cửa sổ Chrome..."
            )
            deadline = time.time() + self.verify_wait_seconds
            while time.time() < deadline:
                time.sleep(2)
                current_url = self.driver.current_url
                if "/verify/traffic/error" not in current_url:
                    print("    [Verify] Đã thoát trang verify, tiếp tục crawl.")
                    return

        body = self.driver.find_element("tag name", "body").text
        message = body.replace("\n", " ")[:300]
        raise RuntimeError(
            "Shopee đang chặn phiên Selenium: "
            f"{current_url}. Nội dung trang: {message}"
        )

    def _api_error_message(self, payload):
        if not isinstance(payload, dict):
            return "response không phải JSON object"

        error = payload.get("error")
        if error in (None, 0):
            return None

        parts = [f"error={error}"]
        for key in ("error_msg", "msg", "tracking_id", "action_type"):
            value = payload.get(key)
            if value not in (None, ""):
                parts.append(f"{key}={value}")
        return ", ".join(parts)

    def _request_api(self, path, params, referer=None):
        url = f"{self.api_base_url}{path}"
        headers = {}
        if referer:
            headers["referer"] = referer

        last_error = None
        for attempt in range(self.api_max_retries + 1):
            try:
                response = self.api_session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.api_timeout_seconds,
                )
                if response.status_code in (401, 403, 429):
                    last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                else:
                    response.raise_for_status()
                    payload = response.json()
                    api_error = self._api_error_message(payload)
                    if not api_error:
                        return payload
                    last_error = api_error
            except requests.RequestException as e:
                last_error = str(e)
            except ValueError as e:
                last_error = f"JSON parse error: {e}"

            if attempt < self.api_max_retries:
                wait_seconds = self.api_retry_delay_seconds * (attempt + 1)
                print(f"    [API] Lỗi Shopee API, thử lại sau {wait_seconds:.1f}s: {last_error}")
                time.sleep(wait_seconds)

        print(f"    [API] Shopee API không trả dữ liệu hợp lệ: {last_error}")
        return None

    def _extract_search_items(self, payload):
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return []

        items = data.get("items") or []
        products = []
        for item in items:
            if not isinstance(item, dict):
                continue

            product = item.get("item_basic") or item.get("item") or item
            if isinstance(product, dict):
                products.append(product)

        return products

    def _get_products_from_api(self, keyword, page=0, limit=None):
        limit = min(limit or self.page_size, 60)
        offset = max(page, 0) * limit
        encoded_keyword = quote(keyword)
        referer = f"{self.api_base_url}/search?keyword={encoded_keyword}&page={page}&sortBy={self.sort_by}"
        params = {
            "by": self.sort_by,
            "keyword": keyword,
            "limit": limit,
            "newest": offset,
            "order": "desc",
            "page_type": "search",
            "scenario": "PAGE_GLOBAL_SEARCH",
            "version": 2,
        }

        payload = self._request_api("/api/v4/search/search_items", params, referer=referer)
        if not payload:
            return []

        raw_products = self._extract_search_items(payload)
        products = []
        for product in raw_products:
            if not product.get("itemid") or not product.get("shopid"):
                continue
            product["_source"] = "shopee_api"
            products.append(self._normalize_product(product, keyword=keyword, page=page))
        print(f"    [API] Tìm thấy {len(products)} sản phẩm từ Shopee API")
        return products[:limit]

    def _clear_performance_logs(self):
        try:
            self.driver.execute_script(
                "if (window.__shopeeCapturedResponses) { window.__shopeeCapturedResponses.length = 0; }"
            )
        except Exception:
            pass

        try:
            self.driver.get_log("performance")
        except Exception:
            pass

    def _get_network_response_body(self, request_id):
        try:
            result = self.driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
        except Exception as e:
            print(f"    [BrowserAPI] Không lấy được response body {request_id}: {e}")
            return None

        body = result.get("body")
        if not body:
            return None

        if result.get("base64Encoded"):
            print("    [BrowserAPI] Bỏ qua response body base64Encoded.")
            return None

        try:
            return json.loads(body)
        except ValueError as e:
            print(f"    [BrowserAPI] Response search_items không parse JSON được: {e}")
            return None

    def _read_search_items_payload_from_browser(self):
        try:
            captured = self.driver.execute_script(
                "return (window.__shopeeCapturedResponses || []).slice();"
            ) or []
        except Exception:
            captured = []

        for item in captured:
            if not isinstance(item, dict):
                continue
            if "/api/v4/search/search_items" not in str(item.get("url", "")):
                continue
            body = item.get("body")
            if body:
                print(f"    [BrowserAPI] Bắt được response search_items từ JS capture: {item.get('url')}")
                return body

        try:
            entries = self.driver.get_log("performance")
        except Exception:
            return None

        for entry in entries:
            try:
                message = json.loads(entry.get("message", "{}"))
                event = message.get("message", {})
            except ValueError:
                continue

            if event.get("method") != "Network.responseReceived":
                continue

            params = event.get("params", {})
            response = params.get("response", {})
            url = response.get("url", "")
            if "/api/v4/search/search_items" not in url:
                continue

            request_id = params.get("requestId")
            if not request_id:
                continue

            print(f"    [BrowserAPI] Bắt được response search_items: {url}")
            payload = self._get_network_response_body(request_id)
            if payload:
                return payload

        return None

    def _build_search_url(self, keyword, page=0):
        encoded_keyword = quote(keyword)
        return f"https://shopee.vn/search?keyword={encoded_keyword}&page={page}&sortBy={self.sort_by}"

    def _open_search_page_for_browser_api(self, keyword, page=0):
        search_url = self._build_search_url(keyword, page=page)
        print(f"    [BrowserAPI] Mở trang search: {search_url}")
        self._clear_performance_logs()
        self._navigate(search_url)

    def _get_products_from_browser_api(self, keyword, page=0, limit=None):
        limit = min(limit or self.page_size, 60)
        self._ensure_driver()
        self._open_search_page_for_browser_api(keyword, page=page)

        deadline = time.time() + self.browser_api_timeout_seconds
        payload = None
        while time.time() < deadline:
            time.sleep(0.5)
            try:
                self._ensure_not_traffic_blocked()
                payload = self._read_search_items_payload_from_browser()
            except WebDriverException as e:
                print(f"    [BrowserAPI] Browser session lỗi khi chờ search_items: {e}")
                return []
            if payload:
                break

        if not payload:
            try:
                print(f"    [BrowserAPI] Không bắt được search_items. current_url={self.driver.current_url}")
                print(f"    [BrowserAPI] title={self.driver.title}")
            except Exception:
                pass
            return []

        api_error = self._api_error_message(payload)
        if api_error:
            print(f"    [BrowserAPI] Shopee page gọi API nhưng response lỗi: {api_error}")
            return []

        raw_products = self._extract_search_items(payload)
        products = []
        for product in raw_products:
            if not product.get("itemid") or not product.get("shopid"):
                continue
            product["_source"] = "browser_api"
            products.append(self._normalize_product(product, keyword=keyword, page=page))
        print(f"    [BrowserAPI] Tìm thấy {len(products)} sản phẩm từ response browser")
        return products[:limit]

    def _navigate(self, url):
        try:
            self.driver.execute_script("window.location.href = arguments[0];", url)
        except WebDriverException as e:
            print(f"    [Warn] Không navigate bằng JS được, fallback driver.get: {e}")
            try:
                self.driver.get(url)
            except TimeoutException:
                print("    [Warn] Page load timeout, tiếp tục đọc DOM hiện có.")

    def _load_search_page(self, keyword, page=0):
        search_key = f"{keyword}:{page}"
        if self.current_search_key == search_key:
            return

        encoded_keyword = quote(keyword)
        search_url = f"https://shopee.vn/search?keyword={encoded_keyword}&page={page}&sortBy={self.sort_by}"
        print(f"    [Selenium] Mở trang search: {search_url}")
        self._navigate(search_url)
        time.sleep(self.search_wait_seconds)
        self._ensure_not_traffic_blocked()

        current_url = self.driver.current_url
        if "/search" not in current_url or "keyword=" not in current_url:
            print("    [Verify] Browser không ở trang search sau verify, reload lại URL search...")
            self._navigate(search_url)
            time.sleep(self.search_wait_seconds)
            self._ensure_not_traffic_blocked()

        self._wait_for_search_dom()
        self._scroll_search_results()
        self.current_search_key = search_key

    def _wait_for_search_dom(self):
        WebDriverWait(self.driver, self.card_wait_seconds).until(
            lambda driver: driver.execute_script("return !!document.body")
        )
        WebDriverWait(self.driver, self.card_wait_seconds).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href], body"))
        )

    def _scroll_search_results(self):
        last_height = 0
        stable_rounds = 0

        for _ in range(self.scroll_rounds):
            try:
                self.driver.execute_script(
                    "if (document.body) { window.scrollBy(0, Math.max(650, window.innerHeight * 0.85)); }"
                )
            except WebDriverException as e:
                print(f"    [Warn] Lỗi scroll, thử tiếp: {e}")

            time.sleep(self.scroll_pause_seconds)
            self._ensure_not_traffic_blocked()

            try:
                height = self.driver.execute_script("return document.body ? document.body.scrollHeight : 0")
            except WebDriverException as e:
                print(f"    [Warn] Không đọc được scrollHeight, thử tiếp: {e}")
                height = 0

            if height == 0:
                continue

            if height == last_height:
                stable_rounds += 1
                if stable_rounds >= 2:
                    break
            else:
                stable_rounds = 0
                last_height = height

    def upload_data_to_minio(self, data, category_name, file_name):
        object_name = f"{self.hive_path}/category={category_name}/{file_name}"

        try:
            json_str = json.dumps(data, ensure_ascii=False, indent=4)
            data_stream = io.BytesIO(json_str.encode("utf-8"))

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_name,
                Body=data_stream,
                ContentType="application/json",
            )
            print(f"    [MinIO-Stream] ✔ Đã upload: {object_name}")
        except Exception as e:
            print(f"    [MinIO-Stream] ✘ Lỗi upload: {e}")

    def _normalize_price(self, value):
        if value is None:
            return 0

        try:
            price = int(value)
        except (TypeError, ValueError):
            return 0

        # Shopee API cũ trả giá theo đơn vị 1/100000 VND; HTML đã là VND thật.
        if abs(price) >= 1_000_000_000:
            return price // 100000

        return price

    def _build_image_url(self, image_id):
        if not image_id:
            return None

        image_id = str(image_id)
        if image_id.startswith("http"):
            return image_id
        if image_id.startswith("//"):
            return f"https:{image_id}"
        if image_id.startswith("data:"):
            return None

        return f"https://down-vn.img.susercontent.com/file/{image_id}"

    def _build_product_url(self, product):
        if product.get("product_url"):
            return product["product_url"]

        item_id = product.get("itemid")
        shop_id = product.get("shopid")

        if not item_id or not shop_id:
            return None

        return f"https://shopee.vn/product/{shop_id}/{item_id}"

    def _normalize_quantity_sold(self, product):
        sold = product.get("sold") or product.get("historical_sold") or 0

        try:
            sold = int(sold)
        except (TypeError, ValueError):
            sold = 0

        return {
            "text": f"Đã bán {sold}",
            "value": sold,
        }

    def _normalize_product(self, product, keyword=None, page=None):
        item_id = product.get("itemid")
        shop_id = product.get("shopid")
        product_url = self._build_product_url(product)
        rating_info = product.get("item_rating") or {}
        price = self._normalize_price(
            product.get("price")
            or product.get("price_min")
            or product.get("price_max")
        )
        original_price = self._normalize_price(
            product.get("price_before_discount")
            or product.get("price_min_before_discount")
            or product.get("price_max_before_discount")
            or product.get("price")
            or product.get("price_min")
        )
        images = product.get("images") or []

        return {
            # Các field chính đặt giống Tiki để tầng xử lý sau có thể đọc chung schema.
            "id": item_id,
            "name": product.get("name"),
            "sku": f"shopee-{shop_id}-{item_id}" if shop_id and item_id else None,
            "original_price": original_price,
            "price": price,
            "quantity_sold": self._normalize_quantity_sold(product),
            "thumbnail_url": self._build_image_url(product.get("image")),
            "brand_name": product.get("brand") or "No Brand",
            "url_key": product_url,
            "url_path": product_url,
            "seller_name": product.get("shop_name"),
            "seller_id": shop_id,
            "seller_product_id": item_id,
            "product_rating": rating_info.get("rating_star"),

            # Giữ field Shopee cũ để code review hiện tại và debug không bị mất ngữ cảnh.
            "itemid": item_id,
            "shopid": shop_id,
            "shop_location": product.get("shop_location"),
            "stock": product.get("stock"),
            "discount": product.get("discount") or product.get("raw_discount"),
            "review_count": product.get("cmt_count"),
            "image_url": self._build_image_url(product.get("image")),
            "images": [self._build_image_url(image_id) for image_id in images if self._build_image_url(image_id)],

            "metadata": {
                "platform": "shopee",
                "keyword": keyword,
                "page": page,
                "source": product.get("_source") or "selenium_html",
                "currency": product.get("currency") or "VND",
                "catid": product.get("catid"),
                "liked_count": product.get("liked_count"),
                "view_count": product.get("view_count"),
                "historical_sold": product.get("historical_sold"),
                "rating_count": rating_info.get("rating_count"),
                "is_official_shop": product.get("is_official_shop"),
                "html_text": product.get("html_text"),
                "raw": product,
            },
        }

    def _extract_product_ids_from_url(self, url):
        if not url:
            return None, None

        patterns = [
            r"-i\.(\d+)\.(\d+)",
            r"/product/(\d+)/(\d+)",
            r"[?&]shopid=(\d+).*?[?&]itemid=(\d+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1), match.group(2)

        return None, None

    def _parse_vnd_price(self, text):
        if not text:
            return 0

        matches = re.findall(r"(?:₫|đ)\s*([0-9][0-9.,]*)", text, flags=re.IGNORECASE)
        if not matches:
            return 0

        value = re.sub(r"\D", "", matches[-1])
        return int(value) if value else 0

    def _parse_sold_count(self, text):
        if not text:
            return 0

        patterns = [
            r"(?:Đã\s*bán|đã\s*bán|sold)\s*([0-9]+(?:[.,][0-9]+)?)(k|K|tr|m)?",
            r"([0-9]+(?:[.,][0-9]+)?)(k|K|tr|m)?\s*(?:đã\s*bán|sold)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue

            number = float(match.group(1).replace(",", "."))
            suffix = (match.group(2) or "").lower()
            if suffix == "k":
                number *= 1_000
            elif suffix in ("tr", "m"):
                number *= 1_000_000
            return int(number)

        return 0

    def _parse_rating(self, text):
        if not text:
            return None

        match = re.search(r"([0-5](?:[.,][0-9])?)\s*(?:/5|sao|star)", text, flags=re.IGNORECASE)
        if not match:
            return None

        return float(match.group(1).replace(",", "."))

    def _clean_product_name(self, card):
        candidates = [
            card.get("title"),
            card.get("aria"),
            card.get("image_alt"),
        ]

        text = card.get("text") or card.get("parent_text") or ""
        for line in text.splitlines():
            line = line.strip()
            if len(line) < 8:
                continue
            if re.search(r"^(₫|đ|đã bán|tài trợ|yêu thích|mall|sale)", line, flags=re.IGNORECASE):
                continue
            if re.search(r"(₫|đã bán|%|giảm)", line, flags=re.IGNORECASE) and len(line) < 30:
                continue
            candidates.append(line)

        for candidate in candidates:
            if candidate:
                cleaned = " ".join(str(candidate).split())
                if cleaned:
                    return cleaned[:300]

        return None

    def _extract_cards_from_html(self):
        script = r"""
            const productUrlPattern = /(-i\.\d+\.\d+|\/product\/\d+\/\d+|[?&]shopid=\d+.*[?&]itemid=\d+)/;
            const textOf = (node) => node ? (node.innerText || node.textContent || '').trim() : '';
            const attr = (node, name) => node ? (node.getAttribute(name) || '') : '';
            const imageUrl = (root) => {
                const images = Array.from(root.querySelectorAll('img'));
                const image = images.find((img) => img.currentSrc || img.src || img.getAttribute('src')) || images[0];
                return image ? (image.currentSrc || image.src || image.getAttribute('src') || '') : '';
            };

            const roots = Array.from(document.querySelectorAll('li.shopee-search-item-result__item, [data-sqe="item"]'));
            const fromRoots = roots.map((root) => {
                const anchor = root.querySelector('a.contents[href], a[href]');
                const href = anchor ? (anchor.href || anchor.getAttribute('href') || '') : '';
                if (!productUrlPattern.test(href)) {
                    return null;
                }

                const nameNode =
                    root.querySelector('div.line-clamp-2.break-words') ||
                    root.querySelector('[class*="line-clamp-2"]') ||
                    root.querySelector('[class*="break-words"]');
                const priceNode =
                    root.querySelector('div.truncate.flex.items-baseline') ||
                    root.querySelector('[class*="items-baseline"]');
                const ratingNode =
                    root.querySelector('[class*="text-shopee-black87"]') ||
                    root.querySelector('[class*="rating"]');
                const locationNode =
                    root.querySelector('[class*="text-shopee-black54"]') ||
                    root.querySelector('[class*="location"]');

                return {
                    href,
                    text: [textOf(nameNode), textOf(priceNode), textOf(ratingNode), textOf(locationNode)].filter(Boolean).join('\\n'),
                    parent_text: textOf(root),
                    title: attr(anchor, 'title'),
                    aria: attr(anchor, 'aria-label'),
                    image_url: imageUrl(root),
                    image_alt: attr(root.querySelector('img'), 'alt'),
                };
            }).filter(Boolean);

            if (fromRoots.length > 0) {
                return fromRoots;
            }

            return Array.from(document.querySelectorAll('a[href]'))
                .map((anchor) => {
                    const href = anchor.href || anchor.getAttribute('href') || '';
                    if (!productUrlPattern.test(href)) {
                        return null;
                    }
                    const cardRoot = anchor.closest('[data-sqe="item"]') || anchor.closest('.shopee-search-item-result__item') || anchor.closest('li') || anchor;
                    return {
                        href,
                        text: textOf(anchor),
                        parent_text: textOf(cardRoot),
                        title: attr(anchor, 'title'),
                        aria: attr(anchor, 'aria-label'),
                        image_url: imageUrl(anchor),
                        image_alt: attr(anchor.querySelector('img'), 'alt'),
                    };
                })
                .filter(Boolean);
        """
        return self.driver.execute_script(script) or []

    def _normalize_html_product(self, card, keyword=None, page=None):
        product_url = urljoin("https://shopee.vn", card.get("href") or "")
        shop_id, item_id = self._extract_product_ids_from_url(product_url)
        text = "\n".join(filter(None, [card.get("text"), card.get("parent_text")]))

        return self._normalize_product(
            {
                "itemid": item_id,
                "shopid": shop_id,
                "name": self._clean_product_name(card),
                "price": self._parse_vnd_price(text),
                "price_before_discount": None,
                "sold": self._parse_sold_count(text),
                "image": card.get("image_url"),
                "images": [card.get("image_url")] if card.get("image_url") else [],
                "brand": None,
                "shop_name": None,
                "shop_location": None,
                "stock": None,
                "discount": None,
                "raw_discount": None,
                "cmt_count": None,
                "item_rating": {"rating_star": self._parse_rating(text)},
                "product_url": product_url,
                "html_text": text,
            },
            keyword=keyword,
            page=page,
        )

    def _get_products_from_html(self, keyword, page=0, limit=None):
        limit = min(limit or self.page_size, 60)
        self._ensure_driver()
        self._load_search_page(keyword, page=page)

        cards = self._extract_cards_from_html()
        products = []
        seen = set()

        for card in cards:
            product = self._normalize_html_product(card, keyword=keyword, page=page)
            item_key = (product.get("shopid"), product.get("itemid"), product.get("url_path"))
            if item_key in seen:
                continue
            seen.add(item_key)

            if not product.get("name") and not product.get("url_path"):
                continue
            products.append(product)
            if len(products) >= limit:
                break

        print(f"    [HTML] Tìm thấy {len(products)} sản phẩm từ DOM")
        if not products:
            try:
                body_text = self.driver.find_element("tag name", "body").text.replace("\n", " ")[:500]
                print(f"    [Debug] current_url={self.driver.current_url}")
                print(f"    [Debug] title={self.driver.title}")
                print(f"    [Debug] body={body_text}")
            except Exception as e:
                print(f"    [Debug] Không đọc được DOM debug: {e}")
        return products

    def get_products(self, keyword, page=0, limit=None):
        if self.fetch_mode in ("api", "api_then_html"):
            products = self._get_products_from_api(keyword, page=page, limit=limit)
            if products or self.fetch_mode == "api":
                return products

            print("    [Fallback] Chuyển sang Selenium HTML vì Shopee API không trả dữ liệu.")
            return self._get_products_from_html(keyword, page=page, limit=limit)

        if self.fetch_mode in ("browser_api", "browser_api_then_html"):
            products = self._get_products_from_browser_api(keyword, page=page, limit=limit)
            if products or self.fetch_mode == "browser_api":
                return products

            print("    [Fallback] Chuyển sang Selenium HTML vì không bắt được response search_items.")
            return self._get_products_from_html(keyword, page=page, limit=limit)

        return self._get_products_from_html(keyword, page=page, limit=limit)

    def _normalize_review(self, review, item_id, shop_id):
        author = review.get("author_username") or review.get("userid") or review.get("authorid")
        content = review.get("comment") or review.get("content") or ""
        product_url = f"{self.api_base_url}/product/{shop_id}/{item_id}"

        return {
            "itemid": item_id,
            "shopid": shop_id,
            "product_url": product_url,
            "rating_star": review.get("rating_star"),
            "author": author,
            "content": content,
            "raw_text": content,
            "like_count": review.get("like_count"),
            "ctime": review.get("ctime"),
            "mtime": review.get("mtime"),
            "source": "shopee_api",
            "crawled_at": datetime.now().isoformat(),
            "metadata": {
                "raw": review,
            },
        }

    def _get_reviews_from_api(self, item_id, shop_id, limit=None, offset=0):
        if not item_id or not shop_id:
            return []

        limit = min(limit or self.review_limit, 50)
        referer = f"{self.api_base_url}/product/{shop_id}/{item_id}"
        params = {
            "filter": 0,
            "flag": 1,
            "itemid": item_id,
            "limit": limit,
            "offset": offset,
            "shopid": shop_id,
            "type": 0,
        }
        payload = self._request_api("/api/v2/item/get_ratings", params, referer=referer)
        if not payload:
            return []

        data = payload.get("data") or {}
        ratings = data.get("ratings") or []
        reviews = [
            self._normalize_review(review, item_id=item_id, shop_id=shop_id)
            for review in ratings
            if isinstance(review, dict)
        ]
        print(f"    [API] Tìm thấy {len(reviews)} review cho {item_id}")
        return reviews

    def _get_reviews_from_html(self, item_id, shop_id, limit=None, offset=0):
        if not item_id or not shop_id:
            return []

        limit = min(limit or self.review_limit, 50)
        product_url = f"https://shopee.vn/product/{shop_id}/{item_id}"
        self._ensure_driver()
        self._navigate(product_url)
        time.sleep(self.detail_wait_seconds)
        self._ensure_not_traffic_blocked()

        for _ in range(max(self.review_pages, 1) * 3):
            self.driver.execute_script("window.scrollBy(0, Math.max(700, window.innerHeight));")
            time.sleep(self.scroll_pause_seconds)
            self._ensure_not_traffic_blocked()

        script = """
            const nodes = Array.from(document.querySelectorAll('[class*="review"], [class*="rating"], [class*="comment"]'));
            const texts = nodes
                .map((node) => (node.innerText || '').trim())
                .filter((text) => text.length >= 20 && text.length <= 1500);
            return Array.from(new Set(texts));
        """
        raw_reviews = self.driver.execute_script(script) or []
        selected = raw_reviews[offset:offset + limit]

        return [
            {
                "itemid": item_id,
                "shopid": shop_id,
                "product_url": product_url,
                "content": text,
                "raw_text": text,
                "source": "selenium_html",
                "crawled_at": datetime.now().isoformat(),
            }
            for text in selected
        ]

    def get_reviews(self, item_id, shop_id, limit=None, offset=0):
        if self.fetch_mode in ("api", "api_then_html"):
            reviews = self._get_reviews_from_api(item_id, shop_id, limit=limit, offset=offset)
            if reviews or self.fetch_mode == "api":
                return reviews

            print(f"    [Fallback] Chuyển sang Selenium HTML để lấy review cho {item_id}.")

        return self._get_reviews_from_html(item_id, shop_id, limit=limit, offset=offset)

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
            return []

        return products[:review_products_limit]

    def crawl_all(self, keyword, start_page=0, end_page=0, review_products_limit=None, review_pages=None):
        try:
            for page in range(start_page, end_page + 1):
                print(f"\n--- Đang cào trang {page} ---")
                products = []
                for attempt in range(self.browser_restart_retries + 1):
                    try:
                        products = self.get_products(keyword, page)
                        break
                    except WebDriverException as e:
                        if attempt >= self.browser_restart_retries:
                            raise
                        print(
                            "    [Warn] Browser session bị mất, "
                            f"khởi tạo lại lần {attempt + 1}/{self.browser_restart_retries}: {e}"
                        )
                        self._refresh_session()

                if not products:
                    print("    ⚠ Không có dữ liệu. Shopee có thể đang chặn session hoặc DOM đã đổi.")
                    continue

                ts = int(time.time())
                p_file = f"batch_pg{page}_{ts}.json"
                self.upload_data_to_minio(products, "products", p_file)

                review_products = self._products_for_reviews(products, review_products_limit)
                print(f"    [Reviews] Sẽ lấy review HTML cho {len(review_products)}/{len(products)} sản phẩm")

                for p in review_products:
                    item_id, shop_id = p.get("itemid"), p.get("shopid")
                    reviews = self.get_all_reviews(item_id, shop_id, max_pages=review_pages)

                    if reviews:
                        r_file = f"reviews_sp_{item_id}_{ts}.json"
                        self.upload_data_to_minio(reviews, "reviews", r_file)

                    self._sleep()

                self._sleep()
        finally:
            self.close()
