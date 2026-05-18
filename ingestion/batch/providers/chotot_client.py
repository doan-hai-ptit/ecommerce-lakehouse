from datetime import datetime
from urllib.parse import urlencode
import io
import json
import os
import random
import time

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

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


class ChototApiClient:
    def __init__(self):
        self.provider = "chotot"
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.hive_path = f"provider={self.provider}/date={self.today}"

        self.base_url = os.getenv(
            "CHOTOT_LISTING_API",
            "https://gateway.chotot.com/v1/public/ad-listing",
        )
        self.timeout = int(os.getenv("CHOTOT_TIMEOUT_SECONDS", "20"))
        default_delay = _get_float_env("CHOTOT_DELAY_SECONDS", 2.0)
        self.min_delay_seconds = _get_float_env("CHOTOT_MIN_DELAY_SECONDS", default_delay)
        self.max_delay_seconds = _get_float_env("CHOTOT_MAX_DELAY_SECONDS", max(default_delay, 5.0))
        self.max_retries = max(_get_int_env("CHOTOT_MAX_RETRIES", 4), 1)
        self.backoff_base_seconds = _get_float_env("CHOTOT_BACKOFF_BASE_SECONDS", 3.0)
        self.last_total = None

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
                "Origin": "https://www.chotot.com",
                "Referer": "https://www.chotot.com/",
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

                if response.status_code in (403, 429):
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

    def get_products(self, page=1, limit=50, keyword=None, category_id=None, region=None, area=None):
        limit = min(max(int(limit), 1), 50)
        offset = max(page - 1, 0) * limit
        params = {
            "limit": limit,
            "o": offset,
        }

        if keyword:
            params["q"] = keyword
        if category_id:
            params["cg"] = category_id
        if region:
            params["region"] = region
        if area:
            params["area"] = area

        data = self._request_json(
            self.base_url,
            params=params,
            error_context=f"fetch Chotot listings page {page}",
        )
        self.last_total = data.get("total")
        return data.get("ads", []) or []

    def get_listings(self, page=1, limit=50, keyword=None, category_id=None, region=None, area=None):
        return self.get_products(
            page=page,
            limit=limit,
            keyword=keyword,
            category_id=category_id,
            region=region,
            area=area,
        )

    def crawl_all(
        self,
        keyword=None,
        category_id=None,
        start_page=1,
        end_page=1,
        limit=50,
        region=None,
        area=None,
    ):
        limit = min(max(int(limit), 1), 50)

        for page in range(start_page, end_page + 1):
            print(f"\n--- Processing CHOTOT PAGE {page} ---")
            offset = max(page - 1, 0) * limit

            if self.last_total is not None and offset >= self.last_total:
                print(f"    Reached total={self.last_total}. Stop crawling this shard.")
                break

            products = self.get_products(
                page=page,
                limit=limit,
                keyword=keyword,
                category_id=category_id,
                region=region,
                area=area,
            )

            if not products:
                print("    No product data.")
                continue

            ts = int(time.time())
            products_file = f"batch_pg{page}_{ts}.json"
            self.upload_data_to_minio(products, "products", products_file)
            self._sleep()
