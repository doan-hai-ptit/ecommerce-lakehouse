import os
import re
import time
import socket
from xml.etree import ElementTree
from urllib.parse import urlparse
import psycopg2
from deltalake import DeltaTable

def sql_identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def quoted_identifier(name):
    return f"`{sql_identifier(name)}`"


def qualified_table_name(db_name, table_name):
    return f"{quoted_identifier(db_name)}.{quoted_identifier(table_name)}"


def hive_site_config(path=None):
    config_path = path or os.getenv("HIVE_SITE_PATH")
    if not config_path or not os.path.exists(config_path):
        # Look in workspace/processing/hive-site.xml or local dir
        possible_paths = [
            "/app/hive-site.xml",
            os.path.join(os.path.dirname(__file__), "hive-site.xml"),
            os.path.join(os.path.dirname(__file__), "../hive-site.xml"),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "hive-site.xml"),
            "/workspace/ecommerce-lakehouse/processing/hive-site.xml"
        ]
        for p in possible_paths:
            if os.path.exists(p):
                config_path = p
                break

    values = {}
    if config_path and os.path.exists(config_path):
        try:
            root = ElementTree.parse(config_path).getroot()
            for prop in root.findall("property"):
                name = prop.findtext("name")
                value = prop.findtext("value")
                if name and value is not None:
                    values[name] = value
        except Exception as e:
            print(f"⚠️ Warning: Failed to parse hive-site.xml: {e}")
    return values


def resolve_db_params(jdbc_url):
    # Remove jdbc: prefix if present
    url = jdbc_url
    if url.startswith("jdbc:"):
        url = url[5:]
    
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    # path starts with '/', so remove it to get database name
    database = parsed.path.lstrip("/") if parsed.path else "postgres_metastore"
    
    # Try resolving hostname, fallback to localhost if cannot resolve
    if host != "localhost":
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            print(f"⚠️ Warning: Cannot resolve database host '{host}'. Falling back to 'localhost'.")
            host = "localhost"
            
    return host, port, database


def metastore_db_config():
    config = hive_site_config()
    jdbc_url = os.getenv("HIVE_METASTORE_JDBC_URL") or config.get("javax.jdo.option.ConnectionURL") or "jdbc:postgresql://postgres:5432/postgres_metastore"
    user = os.getenv("HIVE_METASTORE_JDBC_USER") or config.get("javax.jdo.option.ConnectionUserName") or "postgres"
    password = os.getenv("HIVE_METASTORE_JDBC_PASSWORD") or config.get("javax.jdo.option.ConnectionPassword") or "postgres"
    warehouse = os.getenv("SPARK_WAREHOUSE_DIR") or config.get("hive.metastore.warehouse.dir") or "s3a://silver-lakehouse/warehouse/"
    
    host, port, database = resolve_db_params(jdbc_url)
    
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "warehouse": warehouse
    }


def db_connection(config):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        database=config["database"],
        user=config["user"],
        password=config["password"]
    )


_case = 'upper'

def detect_case(cursor):
    global _case
    try:
        cursor.execute("SAVEPOINT detect_case_sp")
        cursor.execute('SELECT 1 FROM "dbs" LIMIT 1')
        cursor.fetchone()
        cursor.execute("RELEASE SAVEPOINT detect_case_sp")
        _case = 'lower'
    except Exception:
        try:
            cursor.execute("ROLLBACK TO SAVEPOINT detect_case_sp")
        except Exception:
            pass
        _case = 'upper'

def fmt_sql(sql):
    if _case == 'lower':
        return re.sub(r'"([A-Z0-9_]+)"', lambda m: f'"{m.group(1).lower()}"', sql)
    return sql


def db_execute(cursor, sql, params=()):
    cursor.execute(fmt_sql(sql), params)


def db_query_one(cursor, sql, params=()):
    cursor.execute(fmt_sql(sql), params)
    row = cursor.fetchone()
    return row[0] if row else None


def allocate_metastore_id(cursor, sequence_name):
    # Select next_val with row lock
    current_value = db_query_one(
        cursor,
        'SELECT "NEXT_VAL" FROM "SEQUENCE_TABLE" WHERE "SEQUENCE_NAME" = %s FOR UPDATE',
        (sequence_name,),
    )
    if current_value is None:
        current_value = 1
        db_execute(
            cursor,
            'INSERT INTO "SEQUENCE_TABLE" ("SEQUENCE_NAME", "NEXT_VAL") VALUES (%s, %s)',
            (sequence_name, current_value + 5),
        )
    else:
        current_value = int(current_value)
        db_execute(
            cursor,
            'UPDATE "SEQUENCE_TABLE" SET "NEXT_VAL" = %s WHERE "SEQUENCE_NAME" = %s',
            (current_value + 5, sequence_name),
        )
    return current_value


def ensure_metastore_database(cursor, db_name, warehouse_dir, owner="root"):
    db_id = db_query_one(cursor, 'SELECT "DB_ID" FROM "DBS" WHERE "NAME" = %s', (db_name,))
    if db_id is not None:
        return int(db_id)

    db_id = allocate_metastore_id(cursor, "org.apache.hadoop.hive.metastore.model.MDatabase")
    db_location = f"{warehouse_dir.rstrip('/')}/{db_name}.db"
    db_execute(
        cursor,
        (
            'INSERT INTO "DBS" '
            '("DB_ID", "DESC", "DB_LOCATION_URI", "NAME", "OWNER_NAME", "OWNER_TYPE") '
            'VALUES (%s, %s, %s, %s, %s, %s)'
        ),
        (db_id, "", db_location, db_name, owner, "USER"),
    )
    return db_id


def database_location(cursor, db_id, warehouse_dir, db_name):
    location = db_query_one(cursor, 'SELECT "DB_LOCATION_URI" FROM "DBS" WHERE "DB_ID" = %s', (db_id,))
    return str(location or f"{warehouse_dir.rstrip('/')}/{db_name}.db").rstrip("/")


def reset_params(cursor, table_name, id_column, id_value):
    db_execute(cursor, f'DELETE FROM "{table_name}" WHERE "{id_column}" = %s', (id_value,))


def insert_param(cursor, table_name, id_column, id_value, key, value):
    db_execute(
        cursor,
        f'INSERT INTO "{table_name}" ("{id_column}", "PARAM_KEY", "PARAM_VALUE") VALUES (%s, %s, %s)',
        (id_value, key, value),
    )


def insert_schema_params(cursor, tbl_id, schema_json):
    max_param_length = 30000
    if len(schema_json) <= max_param_length:
        insert_param(cursor, "TABLE_PARAMS", "TBL_ID", tbl_id, "spark.sql.sources.schema", schema_json)
        return

    parts = [schema_json[i : i + max_param_length] for i in range(0, len(schema_json), max_param_length)]
    insert_param(cursor, "TABLE_PARAMS", "TBL_ID", tbl_id, "spark.sql.sources.schema.numParts", str(len(parts)))
    for idx, part in enumerate(parts):
        insert_param(cursor, "TABLE_PARAMS", "TBL_ID", tbl_id, f"spark.sql.sources.schema.part.{idx}", part)


def create_metastore_storage(cursor, target_path):
    serde_id = allocate_metastore_id(cursor, "org.apache.hadoop.hive.metastore.model.MSerDeInfo")
    cd_id = allocate_metastore_id(cursor, "org.apache.hadoop.hive.metastore.model.MColumnDescriptor")
    sd_id = allocate_metastore_id(cursor, "org.apache.hadoop.hive.metastore.model.MStorageDescriptor")

    db_execute(
        cursor,
        'INSERT INTO "SERDES" ("SERDE_ID", "NAME", "SLIB") VALUES (%s, %s, %s)',
        (serde_id, None, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    )
    db_execute(cursor, 'INSERT INTO "CDS" ("CD_ID") VALUES (%s)', (cd_id,))
    db_execute(
        cursor,
        (
            'INSERT INTO "COLUMNS_V2" '
            '("CD_ID", "COMMENT", "COLUMN_NAME", "TYPE_NAME", "INTEGER_IDX") '
            'VALUES (%s, %s, %s, %s, %s)'
        ),
        (cd_id, "from deserializer", "col", "array<string>", 0),
    )
    db_execute(
        cursor,
        (
            'INSERT INTO "SDS" '
            '("SD_ID", "CD_ID", "INPUT_FORMAT", "IS_COMPRESSED", "IS_STOREDASSUBDIRECTORIES", '
            '"LOCATION", "NUM_BUCKETS", "OUTPUT_FORMAT", "SERDE_ID") '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        ),
        (
            sd_id,
            cd_id,
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            False,
            False,
            target_path,
            -1,
            "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            serde_id,
        ),
    )
    return sd_id, serde_id


def upsert_spark_datasource_table(cursor, db_id, db_location, table_name, target_path, schema_json, partition_cols):
    tbl_id = db_query_one(
        cursor,
        'SELECT "TBL_ID" FROM "TBLS" WHERE "DB_ID" = %s AND "TBL_NAME" = %s',
        (db_id, table_name),
    )
    now_epoch = int(time.time())

    if tbl_id is None:
        tbl_id = allocate_metastore_id(cursor, "org.apache.hadoop.hive.metastore.model.MTable")
        sd_id, serde_id = create_metastore_storage(cursor, target_path)
        db_execute(
            cursor,
            (
                'INSERT INTO "TBLS" '
                '("TBL_ID", "CREATE_TIME", "DB_ID", "LAST_ACCESS_TIME", "OWNER", "RETENTION", '
                '"IS_REWRITE_ENABLED", "SD_ID", "TBL_NAME", "TBL_TYPE", "VIEW_EXPANDED_TEXT", "VIEW_ORIGINAL_TEXT") '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            ),
            (tbl_id, now_epoch, db_id, 0, "root", 0, False, sd_id, table_name, "EXTERNAL_TABLE", None, None),
        )
    else:
        tbl_id = int(tbl_id)
        existing_provider = db_query_one(
            cursor,
            'SELECT "PARAM_VALUE" FROM "TABLE_PARAMS" WHERE "TBL_ID" = %s AND "PARAM_KEY" = %s',
            (tbl_id, "spark.sql.sources.provider"),
        )
        if existing_provider not in (None, "delta"):
            raise RuntimeError(
                f"Bảng Hive {table_name} đã tồn tại với provider '{existing_provider}', không phải Delta."
            )

        sd_id = db_query_one(cursor, 'SELECT "SD_ID" FROM "TBLS" WHERE "TBL_ID" = %s', (tbl_id,))
        sd_id = int(sd_id)
        serde_id = db_query_one(cursor, 'SELECT "SERDE_ID" FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
        serde_id = int(serde_id)
        db_execute(cursor, 'UPDATE "SDS" SET "LOCATION" = %s WHERE "SD_ID" = %s', (target_path, sd_id))

    reset_params(cursor, "TABLE_PARAMS", "TBL_ID", tbl_id)
    reset_params(cursor, "SERDE_PARAMS", "SERDE_ID", serde_id)

    for key, value in (
        ("EXTERNAL", "TRUE"),
        ("path", target_path),
        ("spark.sql.create.version", "3.5.7"),
        ("spark.sql.partitionProvider", "catalog"),
        ("spark.sql.sources.provider", "delta"),
        ("transient_lastDdlTime", str(now_epoch)),
    ):
        insert_param(cursor, "TABLE_PARAMS", "TBL_ID", tbl_id, key, value)
    insert_schema_params(cursor, tbl_id, schema_json)
    insert_param(cursor, "TABLE_PARAMS", "TBL_ID", tbl_id, "spark.sql.sources.schema.numPartCols", str(len(partition_cols)))
    for idx, partition_col in enumerate(partition_cols):
        insert_param(cursor, "TABLE_PARAMS", "TBL_ID", tbl_id, f"spark.sql.sources.schema.partCol.{idx}", partition_col)

    insert_param(cursor, "SERDE_PARAMS", "SERDE_ID", serde_id, "path", target_path.rstrip("/"))
    insert_param(cursor, "SERDE_PARAMS", "SERDE_ID", serde_id, "serialization.format", "1")


def sync_hive_delta_table(db_name, table_name, target_path, storage_options=None):
    """
    Synchronizes a Delta table to Hive Metastore using python and psycopg2 (completely Spark-free).
    """
    db_config = metastore_db_config()
    
    # Read the Delta Table schema and metadata using deltalake
    try:
        # Standardize path format for delta-rs (must use s3:// instead of s3a:// for S3 storage options)
        s3_path = target_path.replace("s3a://", "s3://")
        dt = DeltaTable(s3_path, storage_options=storage_options)
        schema_json = dt.schema().to_json()
        partition_cols = dt.metadata().partition_columns
    except Exception as e:
        print(f"❌ Error reading Delta table schema at {target_path}: {e}")
        raise e
        
    conn = db_connection(db_config)
    try:
        cursor = conn.cursor()
        detect_case(cursor)
        db_id = ensure_metastore_database(cursor, db_name, db_config["warehouse"])
        db_location = database_location(cursor, db_id, db_config["warehouse"], db_name)
        
        upsert_spark_datasource_table(
            cursor,
            db_id,
            db_location,
            table_name,
            target_path,  # Keep target_path as is (with s3a://) for Spark compatibility
            schema_json,
            partition_cols,
        )
        conn.commit()
        print(f"✓ Successfully synchronized Delta table '{db_name}.{table_name}' to Hive Metastore.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to synchronize '{db_name}.{table_name}' to Hive Metastore: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()
