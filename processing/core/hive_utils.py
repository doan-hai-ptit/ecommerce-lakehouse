import os
import re
import time
from xml.etree import ElementTree
from core.spark_session import get_env

def sql_identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def quoted_identifier(name):
    return f"`{sql_identifier(name)}`"


def qualified_table_name(db_name, table_name):
    return f"{quoted_identifier(db_name)}.{quoted_identifier(table_name)}"


def hive_site_config(path=None):
    config_path = path or os.getenv("HIVE_SITE_PATH", "/opt/spark/conf/hive-site.xml")
    if not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(__file__), "hive-site.xml")
    if not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hive-site.xml")

    values = {}
    if os.path.exists(config_path):
        root = ElementTree.parse(config_path).getroot()
        for prop in root.findall("property"):
            name = prop.findtext("name")
            value = prop.findtext("value")
            if name and value is not None:
                values[name] = value
    return values


def metastore_jdbc_config():
    config = hive_site_config()
    return {
        "url": get_env("HIVE_METASTORE_JDBC_URL", default=config.get("javax.jdo.option.ConnectionURL") or "jdbc:postgresql://postgres:5432/postgres_metastore"),
        "user": get_env("HIVE_METASTORE_JDBC_USER", default=config.get("javax.jdo.option.ConnectionUserName") or "postgres"),
        "password": get_env("HIVE_METASTORE_JDBC_PASSWORD", default=config.get("javax.jdo.option.ConnectionPassword") or "postgres"),
        "warehouse": get_env("SPARK_WAREHOUSE_DIR", default="file:/tmp/spark-warehouse"),
    }


def jdbc_connection(spark, jdbc_config):
    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    return jvm.java.sql.DriverManager.getConnection(
        jdbc_config["url"],
        jdbc_config["user"],
        jdbc_config["password"],
    )


def bind_param(statement, index, value):
    if value is None:
        statement.setNull(index, 12)
    elif isinstance(value, bool):
        statement.setBoolean(index, value)
    elif isinstance(value, int):
        statement.setLong(index, value)
    else:
        statement.setString(index, str(value))


def jdbc_execute(conn, sql, params=()):
    statement = conn.prepareStatement(sql)
    try:
        for idx, value in enumerate(params, start=1):
            bind_param(statement, idx, value)
        statement.executeUpdate()
    finally:
        statement.close()


def jdbc_query_one(conn, sql, params=()):
    statement = conn.prepareStatement(sql)
    try:
        for idx, value in enumerate(params, start=1):
            bind_param(statement, idx, value)
        result = statement.executeQuery()
        try:
            if result.next():
                return result.getString(1)
            return None
        finally:
            result.close()
    finally:
        statement.close()


def allocate_metastore_id(conn, sequence_name):
    current_value = jdbc_query_one(
        conn,
        'SELECT "NEXT_VAL" FROM "SEQUENCE_TABLE" WHERE "SEQUENCE_NAME" = ? FOR UPDATE',
        (sequence_name,),
    )
    if current_value is None:
        current_value = 1
        jdbc_execute(
            conn,
            'INSERT INTO "SEQUENCE_TABLE" ("SEQUENCE_NAME", "NEXT_VAL") VALUES (?, ?)',
            (sequence_name, current_value + 5),
        )
    else:
        current_value = int(current_value)
        jdbc_execute(
            conn,
            'UPDATE "SEQUENCE_TABLE" SET "NEXT_VAL" = ? WHERE "SEQUENCE_NAME" = ?',
            (current_value + 5, sequence_name),
        )
    return current_value


def ensure_metastore_database(conn, db_name, warehouse_dir, owner="root"):
    db_id = jdbc_query_one(conn, 'SELECT "DB_ID" FROM "DBS" WHERE "NAME" = ?', (db_name,))
    if db_id is not None:
        return int(db_id)

    db_id = allocate_metastore_id(conn, "org.apache.hadoop.hive.metastore.model.MDatabase")
    db_location = f"{warehouse_dir.rstrip('/')}/{db_name}.db"
    jdbc_execute(
        conn,
        (
            'INSERT INTO "DBS" '
            '("DB_ID", "DESC", "DB_LOCATION_URI", "NAME", "OWNER_NAME", "OWNER_TYPE") '
            'VALUES (?, ?, ?, ?, ?, ?)'
        ),
        (db_id, "", db_location, db_name, owner, "USER"),
    )
    return db_id


def database_location(conn, db_id, warehouse_dir, db_name):
    location = jdbc_query_one(conn, 'SELECT "DB_LOCATION_URI" FROM "DBS" WHERE "DB_ID" = ?', (db_id,))
    return str(location or f"{warehouse_dir.rstrip('/')}/{db_name}.db").rstrip("/")


def reset_params(conn, table_name, id_column, id_value):
    jdbc_execute(conn, f'DELETE FROM "{table_name}" WHERE "{id_column}" = ?', (id_value,))


def insert_param(conn, table_name, id_column, id_value, key, value):
    jdbc_execute(
        conn,
        f'INSERT INTO "{table_name}" ("{id_column}", "PARAM_KEY", "PARAM_VALUE") VALUES (?, ?, ?)',
        (id_value, key, value),
    )


def insert_schema_params(conn, tbl_id, schema_json):
    max_param_length = 30000
    if len(schema_json) <= max_param_length:
        insert_param(conn, "TABLE_PARAMS", "TBL_ID", tbl_id, "spark.sql.sources.schema", schema_json)
        return

    parts = [schema_json[i : i + max_param_length] for i in range(0, len(schema_json), max_param_length)]
    insert_param(conn, "TABLE_PARAMS", "TBL_ID", tbl_id, "spark.sql.sources.schema.numParts", str(len(parts)))
    for idx, part in enumerate(parts):
        insert_param(conn, "TABLE_PARAMS", "TBL_ID", tbl_id, f"spark.sql.sources.schema.part.{idx}", part)


def create_metastore_storage(conn, target_path):
    serde_id = allocate_metastore_id(conn, "org.apache.hadoop.hive.metastore.model.MSerDeInfo")
    cd_id = allocate_metastore_id(conn, "org.apache.hadoop.hive.metastore.model.MColumnDescriptor")
    sd_id = allocate_metastore_id(conn, "org.apache.hadoop.hive.metastore.model.MStorageDescriptor")

    jdbc_execute(
        conn,
        'INSERT INTO "SERDES" ("SERDE_ID", "NAME", "SLIB") VALUES (?, ?, ?)',
        (serde_id, None, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    )
    jdbc_execute(conn, 'INSERT INTO "CDS" ("CD_ID") VALUES (?)', (cd_id,))
    jdbc_execute(
        conn,
        (
            'INSERT INTO "COLUMNS_V2" '
            '("CD_ID", "COMMENT", "COLUMN_NAME", "TYPE_NAME", "INTEGER_IDX") '
            'VALUES (?, ?, ?, ?, ?)'
        ),
        (cd_id, "from deserializer", "col", "array<string>", 0),
    )
    jdbc_execute(
        conn,
        (
            'INSERT INTO "SDS" '
            '("SD_ID", "CD_ID", "INPUT_FORMAT", "IS_COMPRESSED", "IS_STOREDASSUBDIRECTORIES", '
            '"LOCATION", "NUM_BUCKETS", "OUTPUT_FORMAT", "SERDE_ID") '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
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


def upsert_spark_datasource_table(conn, db_id, db_location, table_name, target_path, schema_json, partition_cols):
    tbl_id = jdbc_query_one(
        conn,
        'SELECT "TBL_ID" FROM "TBLS" WHERE "DB_ID" = ? AND "TBL_NAME" = ?',
        (db_id, table_name),
    )
    now_epoch = int(time.time())

    if tbl_id is None:
        tbl_id = allocate_metastore_id(conn, "org.apache.hadoop.hive.metastore.model.MTable")
        sd_id, serde_id = create_metastore_storage(conn, target_path)
        jdbc_execute(
            conn,
            (
                'INSERT INTO "TBLS" '
                '("TBL_ID", "CREATE_TIME", "DB_ID", "LAST_ACCESS_TIME", "OWNER", "RETENTION", '
                '"IS_REWRITE_ENABLED", "SD_ID", "TBL_NAME", "TBL_TYPE", "VIEW_EXPANDED_TEXT", "VIEW_ORIGINAL_TEXT") '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            ),
            (tbl_id, now_epoch, db_id, 0, "root", 0, False, sd_id, table_name, "EXTERNAL_TABLE", None, None),
        )
    else:
        tbl_id = int(tbl_id)
        existing_provider = jdbc_query_one(
            conn,
            'SELECT "PARAM_VALUE" FROM "TABLE_PARAMS" WHERE "TBL_ID" = ? AND "PARAM_KEY" = ?',
            (tbl_id, "spark.sql.sources.provider"),
        )
        if existing_provider not in (None, "delta"):
            raise RuntimeError(
                f"Bảng Hive {table_name} đã tồn tại với provider '{existing_provider}', không phải Delta."
            )

        sd_id = jdbc_query_one(conn, 'SELECT "SD_ID" FROM "TBLS" WHERE "TBL_ID" = ?', (tbl_id,))
        sd_id = int(sd_id)
        serde_id = jdbc_query_one(conn, 'SELECT "SERDE_ID" FROM "SDS" WHERE "SD_ID" = ?', (sd_id,))
        serde_id = int(serde_id)
        jdbc_execute(conn, 'UPDATE "SDS" SET "LOCATION" = ? WHERE "SD_ID" = ?', (target_path, sd_id))

    reset_params(conn, "TABLE_PARAMS", "TBL_ID", tbl_id)
    reset_params(conn, "SERDE_PARAMS", "SERDE_ID", serde_id)

    for key, value in (
        ("EXTERNAL", "TRUE"),
        ("spark.sql.create.version", "3.5.7"),
        ("spark.sql.partitionProvider", "catalog"),
        ("spark.sql.sources.provider", "delta"),
        ("transient_lastDdlTime", str(now_epoch)),
    ):
        insert_param(conn, "TABLE_PARAMS", "TBL_ID", tbl_id, key, value)
    insert_schema_params(conn, tbl_id, schema_json)
    insert_param(conn, "TABLE_PARAMS", "TBL_ID", tbl_id, "spark.sql.sources.schema.numPartCols", str(len(partition_cols)))
    for idx, partition_col in enumerate(partition_cols):
        insert_param(conn, "TABLE_PARAMS", "TBL_ID", tbl_id, f"spark.sql.sources.schema.partCol.{idx}", partition_col)


def sync_hive_delta_table(spark, db_name, table_name, target_path):
    """
    Đăng ký Delta table vào Hive Metastore bằng metadata Spark datasource table.

    Không dùng Spark SQL CREATE TABLE vì Spark/Hive bị kẹt ở bước tạo thư mục
    *-__PLACEHOLDER__ trên warehouse trong môi trường Docker/MinIO hiện tại.
    """
    jdbc_config = metastore_jdbc_config()
    delta_df = spark.read.format("delta").load(target_path)
    schema_json = delta_df.schema.json()
    if "partition_date" in delta_df.columns:
        partition_cols = ["partition_date"]
    elif "event_date" in delta_df.columns:
        partition_cols = ["event_date"]
    else:
        partition_cols = []

    conn = jdbc_connection(spark, jdbc_config)
    try:
        conn.setAutoCommit(False)
        db_id = ensure_metastore_database(conn, db_name, jdbc_config["warehouse"])
        db_location = database_location(conn, db_id, jdbc_config["warehouse"], db_name)
        upsert_spark_datasource_table(
            conn,
            db_id,
            db_location,
            table_name,
            target_path,
            schema_json,
            partition_cols,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
