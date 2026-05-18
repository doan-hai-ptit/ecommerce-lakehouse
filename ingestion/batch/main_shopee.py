from providers.shopee_client import ShopeeApiClient
import argparse
import os


def _first_existing_path(paths):
    for path in paths:
        if os.path.exists(path):
            return path

    return None


def configure_shopee_runtime(args):
    os.environ.setdefault("SHOPEE_DRIVER", args.driver)
    os.environ.setdefault("SHOPEE_HEADLESS", "true" if args.headless else "false")
    os.environ.setdefault("SHOPEE_OPEN_SEARCH_PAGE", "true" if args.open_search_page else "false")
    os.environ.setdefault("SHOPEE_VERIFY_WAIT_SECONDS", str(args.verify_wait_seconds))
    os.environ.setdefault("SHOPEE_USER_DATA_DIR", args.user_data_dir)

    if args.browser_binary:
        os.environ.setdefault("SHOPEE_BROWSER_BINARY", args.browser_binary)
        return

    browser_binary = _first_existing_path([
        "/opt/coccoc/browser/browser",
        "/opt/coccoc/browser/coccoc-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ])
    if browser_binary:
        os.environ.setdefault("SHOPEE_BROWSER_BINARY", browser_binary)


def main():
    print("=== Shopee Ingestion - ecommerce-lakehouse ===")

    parser = argparse.ArgumentParser(description="Shopee keyword crawler")
    parser.add_argument("--keyword", type=str, required=True, help="Search keyword, e.g. dien thoai, sua rua mat")
    parser.add_argument("--start_page", type=int, default=0, help="Start page. Shopee starts from 0")
    parser.add_argument("--end_page", type=int, default=0, help="End page. Shopee starts from 0")
    parser.add_argument(
        "--review_products_limit",
        type=int,
        default=None,
        help="Number of products per page to crawl reviews for. Use 0 to skip reviews",
    )
    parser.add_argument("--review_pages", type=int, default=None, help="Review pages per product")
    parser.add_argument("--driver", choices=["local", "browserless"], default="local", help="Selenium driver mode")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--browser_binary", type=str, default=None, help="Chrome/Chromium/CocCoc binary path")
    parser.add_argument("--user_data_dir", type=str, default=".shopee-chrome-profile", help="Browser profile directory")
    parser.add_argument("--verify_wait_seconds", type=int, default=180, help="Seconds to wait for manual login/verify")
    parser.add_argument(
        "--no_open_search_page",
        dest="open_search_page",
        action="store_false",
        help="Do not open Shopee search page before calling API",
    )
    parser.set_defaults(open_search_page=True)
    args = parser.parse_args()

    configure_shopee_runtime(args)

    client = ShopeeApiClient()
    client.crawl_all(
        keyword=args.keyword,
        start_page=args.start_page,
        end_page=args.end_page,
        review_products_limit=args.review_products_limit,
        review_pages=args.review_pages,
    )


if __name__ == "__main__":
    main()
