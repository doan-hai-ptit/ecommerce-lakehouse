from providers.chotot_client import ChototApiClient
import argparse
import os


def main():
    print("=== Chotot Ingestion - ecommerce-lakehouse ===")

    parser = argparse.ArgumentParser(description="Chotot Crawler")
    parser.add_argument("--keyword", type=str, default=None, help="Search keyword, e.g. iphone")
    parser.add_argument("--category", type=int, default=None, help="Chotot category id, e.g. 5000")
    parser.add_argument("--start_page", type=int, default=1, help="Start page")
    parser.add_argument("--end_page", type=int, default=1, help="End page")
    parser.add_argument("--limit", type=int, default=50, help="Listings per page")
    parser.add_argument("--region", type=int, default=None, help="Chotot region id filter")
    parser.add_argument("--area", type=int, default=None, help="Chotot area id filter")
    parser.add_argument("--min_delay", type=float, default=None, help="Minimum delay between requests")
    parser.add_argument("--max_delay", type=float, default=None, help="Maximum delay between requests")
    parser.add_argument("--max_retries", type=int, default=None, help="Max retries for temporary HTTP errors")
    args = parser.parse_args()

    if args.min_delay is not None:
        os.environ["CHOTOT_MIN_DELAY_SECONDS"] = str(args.min_delay)
    if args.max_delay is not None:
        os.environ["CHOTOT_MAX_DELAY_SECONDS"] = str(args.max_delay)
    if args.max_retries is not None:
        os.environ["CHOTOT_MAX_RETRIES"] = str(args.max_retries)

    client = ChototApiClient()
    client.crawl_all(
        keyword=args.keyword,
        category_id=args.category,
        start_page=args.start_page,
        end_page=args.end_page,
        limit=args.limit,
        region=args.region,
        area=args.area,
    )


if __name__ == "__main__":
    main()
