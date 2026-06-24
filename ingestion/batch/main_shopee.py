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
    os.environ.setdefault("SHOPEE_MANUAL_VERIFY", "true" if args.manual_verify else "false")
    os.environ.setdefault("SHOPEE_SORT_BY", args.sort_by)
    os.environ.setdefault("SHOPEE_FETCH_MODE", args.fetch_mode)
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
    parser.add_argument("--driver", choices=["local", "browserless", "undetected"], default="local", help="Selenium driver mode")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--browser_binary", type=str, default=None, help="Chrome/Chromium/CocCoc binary path")
    parser.add_argument("--user_data_dir", type=str, default=".shopee-chrome-profile", help="Browser profile directory")
    parser.add_argument("--verify_wait_seconds", type=int, default=90, help="Seconds to wait if Shopee traffic verify appears")
    parser.add_argument("--manual_verify", action="store_true", help="Pause for manual captcha/traffic verification when Shopee blocks the browser")
    parser.add_argument("--sort_by", type=str, default="sales", help="Shopee search sort mode, e.g. sales or relevancy")
    parser.add_argument(
        "--fetch_mode",
        choices=["api", "html", "api_then_html", "browser_api", "browser_api_then_html"],
        default="api_then_html",
        help="Fetch mode: direct API, Selenium DOM, browser-captured API, or fallback variants",
    )
    parser.add_argument(
        "--no_open_search_page",
        dest="open_search_page",
        action="store_false",
        help="Deprecated; HTML crawler always opens the search page",
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
