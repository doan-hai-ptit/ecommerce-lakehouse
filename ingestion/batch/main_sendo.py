from providers.sendo_client import SendoApiClient
import argparse


def main():
    print("=== Sendo Ingestion - ecommerce-lakehouse ===")

    parser = argparse.ArgumentParser(description="Sendo Farm grocery crawler")
    parser.add_argument("--keyword", type=str, required=True, help="Sendo Farm keyword, e.g. sua, gao, rau")
    parser.add_argument("--category", type=int, default=None, help="Sendo Farm category id filter")
    parser.add_argument("--start_page", type=int, default=1, help="Start page")
    parser.add_argument("--end_page", type=int, default=1, help="End page")
    parser.add_argument("--limit", type=int, default=40, help="Products per page")
    args = parser.parse_args()

    client = SendoApiClient()
    client.crawl_all(
        keyword=args.keyword,
        category_id=args.category,
        start_page=args.start_page,
        end_page=args.end_page,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
