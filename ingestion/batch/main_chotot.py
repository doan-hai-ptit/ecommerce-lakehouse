from providers.chotot_client import ChototApiClient
import argparse


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
    args = parser.parse_args()

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
