from datetime import datetime
from urllib.parse import urlencode
import io
import json
import os
import time

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()


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
        self.platform = os.getenv("SENDO_PLATFORM", "web")
        self.station_code = os.getenv("SENDO_STATION_CODE")
        self.device_id = os.getenv("SENDO_DEVICE_ID")
        self.timeout = int(os.getenv("SENDO_TIMEOUT_SECONDS", "20"))
        self.delay_seconds = float(os.getenv("SENDO_DELAY_SECONDS", "2.0"))

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

    def _request_json(self, url, params=None, error_context="request"):
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            query = f"?{urlencode(params or {})}" if params else ""
            print(f"Error {error_context}: {url}{query} | {e}")
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

    def get_products(self, keyword, page=1, limit=40, category_id=None):
        api_page = max(page - 1, 0)
        params = {
            "product_name": keyword,
            "page": api_page,
            "region_id": self.region_id,
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
            error_context=f"fetch Sendo products page {page}",
        )
        products = self._extract_products(data)
        return products[:limit] if limit and limit > 0 else products

    def _print_empty_products_hint(self, keyword, page, category_id):
        category_hint = f", category_id={category_id}" if category_id else ""
        print(
            "    No product data from Sendo Farm "
            f"(keyword='{keyword}', page={page}, region_id={self.region_id}{category_hint})."
        )
        print("    This source is grocery-only; try keywords like 'sua', 'gao', 'rau'.")
        print("    For electronics such as 'iphone', use another marketplace provider in this project.")

    def crawl_all(self, keyword, start_page=1, end_page=1, limit=40, category_id=None):
        for page in range(start_page, end_page + 1):
            print(f"\n--- Processing SENDO PAGE {page} ---")
            products = self.get_products(
                keyword=keyword,
                page=page,
                limit=limit,
                category_id=category_id,
            )

            if not products:
                self._print_empty_products_hint(keyword, page, category_id)
                continue

            ts = int(time.time())
            products_file = f"products_pg{page}_{ts}.json"
            self.upload_data_to_minio(products, "products", products_file)
            time.sleep(self.delay_seconds)
