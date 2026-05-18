from datetime import datetime
from urllib.parse import urlencode
import io
import json
import os
import random
import re
import time

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()


DEFAULT_SENDO_KEYWORDS = [
    "sua",
    "gao",
    "rau",
    "thit",
    "ca",
    "trung",
    "mi goi",
    "dau an",
    "nuoc mam",
    "nuoc tuong",
    "hat nem",
    "duong",
    "muoi",
    "nuoc ngot",
    "bia",
    "tra",
    "ca phe",
    "banh",
    "keo",
    "snack",
    "nuoc giat",
    "nuoc rua chen",
    "dau goi",
    "sua tam",
    "giay ve sinh",
    "khau trang",
]


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


def _split_csv(value):
    if not value:
        return []

    return [item.strip() for item in value.split(",") if item.strip()]


class SendoApiClient:
    def __init__(self):
        self.provider = "sendo"
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.hive_path = f"provider={self.provider}/date={self.today}"

        self.products_api = os.getenv(
            "SENDO_PRODUCTS_API",
            "https://grocery.sendo.vn/api/v2/product/search",
        )
        self.region_id = os.getenv("SENDO_REGION_ID", "1")
        self.region_ids = _split_csv(os.getenv("SENDO_REGION_IDS")) or [self.region_id]
        self.platform = os.getenv("SENDO_PLATFORM", "web")
        self.station_code = os.getenv("SENDO_STATION_CODE")
        self.device_id = os.getenv("SENDO_DEVICE_ID")
        self.timeout = _get_int_env("SENDO_TIMEOUT_SECONDS", 20)
        default_delay = _get_float_env("SENDO_DELAY_SECONDS", 2.0)
        self.min_delay_seconds = _get_float_env("SENDO_MIN_DELAY_SECONDS", default_delay)
        self.max_delay_seconds = _get_float_env("SENDO_MAX_DELAY_SECONDS", max(default_delay, 5.0))
        self.max_retries = max(_get_int_env("SENDO_MAX_RETRIES", 4), 1)
        self.backoff_base_seconds = _get_float_env("SENDO_BACKOFF_BASE_SECONDS", 3.0)
        self.stop_after_empty_pages = max(_get_int_env("SENDO_STOP_AFTER_EMPTY_PAGES", 1), 1)

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

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Origin": "https://www.sendo.vn",
                "Referer": "https://www.sendo.vn/",
            }
        )

    def _sleep(self):
        max_delay = max(self.max_delay_seconds, self.min_delay_seconds)
        time.sleep(random.uniform(self.min_delay_seconds, max_delay))

    def _request_json(self, url, params=None, error_context="request"):
        query = f"?{urlencode(params or {})}" if params else ""

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)

                if response.status_code == 200:
                    return response.json()

                if response.status_code in (403, 429, 500, 502, 503, 504):
                    retry_after = response.headers.get("Retry-After")
                    wait_seconds = float(retry_after) if retry_after else self.backoff_base_seconds * attempt
                    print(
                        f"Warn {error_context}: HTTP {response.status_code}. "
                        f"Sleeping {wait_seconds:.1f}s before retry {attempt}/{self.max_retries}"
                    )
                    time.sleep(wait_seconds + random.uniform(0, self.backoff_base_seconds))
                    continue

                response.raise_for_status()
                return response.json()
            except Exception as e:
                print(f"Error {error_context}: {url}{query} | attempt {attempt}/{self.max_retries} | {e}")
                if attempt < self.max_retries:
                    time.sleep(self.backoff_base_seconds * attempt + random.uniform(0, self.backoff_base_seconds))

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
                ContentType="application/json",
            )
            print(f"    [MinIO-Stream] Uploaded: {object_name}")
        except Exception as e:
            print(f"    [MinIO-Stream] Upload error: {e}")

    def _extract_products(self, data):
        if isinstance(data, list):
            return data

        if not isinstance(data, dict):
            return []

        for key in ("data", "products", "items", "result"):
            value = data.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                if isinstance(value.get("list"), list):
                    return value.get("list") or []

                nested = self._extract_products(value)
                if nested:
                    return nested

        return []

    def _safe_part(self, value):
        value = str(value or "all").strip().lower()
        value = re.sub(r"[^a-z0-9_-]+", "-", value)
        return value.strip("-") or "all"

    def _with_crawl_metadata(self, product, keyword, page, region_id, category_id):
        enriched = dict(product)
        enriched["crawl_keyword"] = keyword
        enriched["crawl_page"] = page
        enriched["crawl_region_id"] = str(region_id)
        enriched["crawl_category_id"] = category_id
        enriched["source_provider"] = self.provider
        return enriched

    def get_products(self, keyword, page=1, limit=40, category_id=None, region_id=None):
        api_page = max(page - 1, 0)
        region_id = region_id or self.region_id
        params = {
            "product_name": keyword,
            "page": api_page,
            "region_id": region_id,
            "platform": self.platform,
        }

        if category_id:
            params["category_id"] = category_id
        if self.station_code:
            params["station_code"] = self.station_code
        if self.device_id:
            params["device_id"] = self.device_id

        data = self._request_json(
            self.products_api,
            params=params,
            error_context=f"fetch Sendo products keyword='{keyword}' page {page} region {region_id}",
        )
        products = self._extract_products(data)
        if limit and limit > 0:
            products = products[:limit]

        return [
            self._with_crawl_metadata(product, keyword, page, region_id, category_id)
            for product in products
        ]

    def _print_empty_products_hint(self, keyword, page, category_id, region_id):
        category_hint = f", category_id={category_id}" if category_id else ""
        print(
            "    No product data from Sendo Farm "
            f"(keyword='{keyword}', page={page}, region_id={region_id}{category_hint})."
        )

    def crawl_keyword(self, keyword, start_page=1, end_page=10, limit=40, category_id=None, region_id=None):
        empty_pages = 0
        region_id = region_id or self.region_id

        for page in range(start_page, end_page + 1):
            print(f"\n--- Processing SENDO keyword='{keyword}' region={region_id} page={page} ---")
            products = self.get_products(
                keyword=keyword,
                page=page,
                limit=limit,
                category_id=category_id,
                region_id=region_id,
            )

            if not products:
                empty_pages += 1
                self._print_empty_products_hint(keyword, page, category_id, region_id)
                if empty_pages >= self.stop_after_empty_pages:
                    print(f"    Stop keyword='{keyword}' after {empty_pages} empty page(s).")
                    break

                self._sleep()
                continue

            empty_pages = 0
            ts = int(time.time())
            keyword_part = self._safe_part(keyword)
            products_file = f"batch_pg{page}_{ts}_kw-{keyword_part}_region-{region_id}.json"
            self.upload_data_to_minio(products, "products", products_file)
            self._sleep()

    def crawl_all(
        self,
        keyword=None,
        keywords=None,
        start_page=1,
        end_page=10,
        limit=40,
        category_id=None,
        region_ids=None,
    ):
        if keywords is None:
            keywords = [keyword] if keyword else DEFAULT_SENDO_KEYWORDS

        keywords = [item.strip() for item in keywords if item and item.strip()]
        region_ids = [str(item).strip() for item in (region_ids or self.region_ids) if str(item).strip()]

        if not keywords:
            raise ValueError("Sendo crawl needs at least one keyword.")

        for region_id in region_ids:
            for current_keyword in keywords:
                self.crawl_keyword(
                    keyword=current_keyword,
                    start_page=start_page,
                    end_page=end_page,
                    limit=limit,
                    category_id=category_id,
                    region_id=region_id,
                )
