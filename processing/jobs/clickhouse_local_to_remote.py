import argparse
import os
import urllib.parse
import urllib.request

from dotenv import load_dotenv

load_dotenv()


DEFAULT_DATABASES = ["gold_serving", "silver_real_serving"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Copy local ClickHouse serving databases to a remote ClickHouse server."
    )
    parser.add_argument("--local-host", default=os.getenv("LOCAL_CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--local-port", type=int, default=int(os.getenv("LOCAL_CLICKHOUSE_PORT", "8123")))
    parser.add_argument("--local-user", default=os.getenv("LOCAL_CLICKHOUSE_USER", "admin"))
    parser.add_argument("--local-password", default=os.getenv("LOCAL_CLICKHOUSE_PASSWORD", "password123"))
    parser.add_argument("--remote-host", default=os.getenv("REMOTE_CLICKHOUSE_HOST"))
    parser.add_argument("--remote-port", type=int, default=int(os.getenv("REMOTE_CLICKHOUSE_PORT", "8123")))
    parser.add_argument("--remote-user", default=os.getenv("REMOTE_CLICKHOUSE_USER", "admin"))
    parser.add_argument("--remote-password", default=os.getenv("REMOTE_CLICKHOUSE_PASSWORD"))
    parser.add_argument(
        "--databases",
        default=os.getenv("CLICKHOUSE_SYNC_DATABASES", ",".join(DEFAULT_DATABASES)),
        help="Comma-separated databases to copy. Default: gold_serving,silver_real_serving.",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Optional comma-separated table allow-list applied to every selected database.",
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="replace truncates remote tables before insert. append only inserts rows.",
    )
    parser.add_argument(
        "--secure",
        action="store_true",
        default=os.getenv("REMOTE_CLICKHOUSE_SECURE", "false").lower() == "true",
        help="Use HTTPS for the remote ClickHouse HTTP endpoint.",
    )
    return parser.parse_args()


def http_query(host, port, user, password, query, database=None, body=None, secure=False):
    scheme = "https" if secure else "http"
    params = {
        "user": user,
        "password": password or "",
        "query": query,
    }
    if database:
        params["database"] = database

    url = f"{scheme}://{host}:{port}/?{urllib.parse.urlencode(params)}"
    data = body.encode("utf-8") if isinstance(body, str) else body
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            return response.read()
    except Exception as exc:
        if hasattr(exc, "read"):
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse query failed: {details}") from exc
        raise


def local_query(args, query, database=None):
    return http_query(
        args.local_host,
        args.local_port,
        args.local_user,
        args.local_password,
        query,
        database=database,
    )


def remote_query(args, query, database=None, body=None):
    return http_query(
        args.remote_host,
        args.remote_port,
        args.remote_user,
        args.remote_password,
        query,
        database=database,
        body=body,
        secure=args.secure,
    )


def parse_lines(raw):
    return [line for line in raw.decode("utf-8").splitlines() if line.strip()]


def get_create_table(args, database, table_name):
    raw = local_query(args, f"SHOW CREATE TABLE {database}.{table_name} FORMAT TSVRaw")
    return raw.decode("utf-8").strip()


def copy_table(args, database, table_name):
    print(f"Syncing {database}.{table_name} ...")
    ddl = get_create_table(args, database, table_name)

    remote_query(args, f"CREATE DATABASE IF NOT EXISTS {database}")
    remote_query(args, ddl, database=database)

    if args.mode == "replace":
        remote_query(args, f"TRUNCATE TABLE {database}.{table_name}", database=database)

    data = local_query(args, f"SELECT * FROM {database}.{table_name} FORMAT JSONEachRow")
    if not data.strip():
        print("  No rows to insert.")
        return

    remote_query(
        args,
        f"INSERT INTO {database}.{table_name} FORMAT JSONEachRow",
        database=database,
        body=data,
    )

    count = remote_query(args, f"SELECT count() FROM {database}.{table_name}", database=database)
    print(f"  Remote rows: {count.decode('utf-8').strip()}")


def main():
    args = parse_args()
    if not args.remote_host:
        raise SystemExit("Missing --remote-host or REMOTE_CLICKHOUSE_HOST.")
    if args.remote_password is None:
        raise SystemExit("Missing --remote-password or REMOTE_CLICKHOUSE_PASSWORD.")

    databases = [db.strip() for db in args.databases.split(",") if db.strip()]
    table_filter = None
    if args.tables:
        table_filter = {t.strip() for t in args.tables.split(",") if t.strip()}

    print(f"Local ClickHouse:  {args.local_host}:{args.local_port}")
    print(f"Remote ClickHouse: {args.remote_host}:{args.remote_port}")
    print(f"Mode: {args.mode}")

    for database in databases:
        raw_tables = local_query(args, f"SHOW TABLES FROM {database}")
        tables = parse_lines(raw_tables)
        if table_filter:
            tables = [table for table in tables if table in table_filter]

        for table_name in tables:
            copy_table(args, database, table_name)


if __name__ == "__main__":
    main()
