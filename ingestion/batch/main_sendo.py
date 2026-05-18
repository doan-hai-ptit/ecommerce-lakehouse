from providers.sendo_client import DEFAULT_SENDO_KEYWORDS, SendoApiClient
import argparse
import os


def _split_csv(value):
    if not value:
        return []

    return [item.strip() for item in value.split(",") if item.strip()]


def _read_keywords_file(path):
    if not path:
        return []

    with open(path, "r", encoding="utf-8") as file:
        return [
            line.strip()
            for line in file
            if line.strip() and not line.strip().startswith("#")
        ]


def _build_keywords(args):
    keywords = []

    if args.keyword:
        keywords.append(args.keyword)

    keywords.extend(_split_csv(args.keywords))
    keywords.extend(_read_keywords_file(args.keywords_file))

    if not keywords and args.use_default_keywords:
        keywords.extend(DEFAULT_SENDO_KEYWORDS)

    seen = set()
    unique_keywords = []
    for keyword in keywords:
        key = keyword.lower()
        if key not in seen:
            unique_keywords.append(keyword)
            seen.add(key)

    return unique_keywords


def main():
    print("=== Sendo Farm Ingestion - ecommerce-lakehouse ===")

    parser = argparse.ArgumentParser(description="Sendo Farm grocery/FMCG crawler")
    parser.add_argument("--keyword", type=str, default=None, help="Single keyword, e.g. sua")
    parser.add_argument("--keywords", type=str, default=None, help="Comma-separated keywords, e.g. sua,gao,rau")
    parser.add_argument("--keywords_file", type=str, default=None, help="Text file with one keyword per line")
    parser.add_argument(
        "--use_default_keywords",
        action="store_true",
        help="Use built-in grocery/FMCG keywords when no keyword is provided",
    )
    parser.add_argument("--category", type=int, default=None, help="Sendo Farm category id filter")
    parser.add_argument("--regions", type=str, default=None, help="Comma-separated region ids, e.g. 1,2")
    parser.add_argument("--start_page", type=int, default=1, help="Start page")
    parser.add_argument("--end_page", type=int, default=10, help="End page")
    parser.add_argument("--limit", type=int, default=40, help="Products per page. Sendo Farm usually returns 20")
    parser.add_argument("--min_delay", type=float, default=None, help="Minimum delay between requests")
    parser.add_argument("--max_delay", type=float, default=None, help="Maximum delay between requests")
    parser.add_argument("--max_retries", type=int, default=None, help="Max retries for temporary HTTP errors")
    parser.add_argument("--stop_after_empty_pages", type=int, default=None, help="Stop a keyword after N empty pages")
    args = parser.parse_args()

    if args.min_delay is not None:
        os.environ["SENDO_MIN_DELAY_SECONDS"] = str(args.min_delay)
    if args.max_delay is not None:
        os.environ["SENDO_MAX_DELAY_SECONDS"] = str(args.max_delay)
    if args.max_retries is not None:
        os.environ["SENDO_MAX_RETRIES"] = str(args.max_retries)
    if args.stop_after_empty_pages is not None:
        os.environ["SENDO_STOP_AFTER_EMPTY_PAGES"] = str(args.stop_after_empty_pages)

    keywords = _build_keywords(args)
    if not keywords:
        keywords = DEFAULT_SENDO_KEYWORDS

    client = SendoApiClient()
    client.crawl_all(
        keywords=keywords,
        region_ids=_split_csv(args.regions) or None,
        category_id=args.category,
        start_page=args.start_page,
        end_page=args.end_page,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
