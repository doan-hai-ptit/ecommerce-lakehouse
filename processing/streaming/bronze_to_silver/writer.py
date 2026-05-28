from delta.tables import DeltaTable
from pyspark.sql import functions as F
from jobs.bronze_to_postgres import sync_hive_delta_table
from .utils import table_path, output_columns, non_delete_rows, has_rows
from .transformer import normalize_table_events


def ensure_hive_table(spark, hive_db, table_name, target_path):
    sync_hive_delta_table(spark, hive_db, table_name, target_path)


def initialize_delta_table(df, spec, target_path):
    initial_rows = non_delete_rows(df, spec)
    if not has_rows(initial_rows):
        return False

    (
        initial_rows.repartition("event_date")
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .save(target_path)
    )
    return True


def merge_delta_table(df, spec, target_path):
    delta_table = DeltaTable.forPath(df.sparkSession, target_path)
    merge_condition = " AND ".join([f"target.`{key}` = source.`{key}`" for key in spec.primary_keys])

    values = {column: f"source.`{column}`" for column in output_columns(spec)}

    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedDelete(condition="source._change_op = 'd'")
        .whenMatchedUpdate(condition="source._change_op <> 'd'", set=values)
        .whenNotMatchedInsert(condition="source._change_op <> 'd'", values=values)
        .execute()
    )


def process_table(batch_df, table_name, spec, args):
    table_df = batch_df.where(F.col("source_table") == table_name)
    normalized_df = normalize_table_events(table_df, table_name, spec).persist()
    target_path = table_path(args.silver_base, table_name)

    try:
        raw_count = table_df.count()
        valid_count = normalized_df.count()
        if valid_count == 0:
            print(f"  - {table_name}: raw={raw_count}, hợp lệ=0, bỏ qua vì thiếu khóa chính.")
            return

        op_counts = {
            row["_change_op"]: row["count"]
            for row in normalized_df.groupBy("_change_op").count().collect()
        }
        print(
            f"  - {table_name}: raw={raw_count}, hợp lệ={valid_count}, "
            f"ops={op_counts}, path={target_path}"
        )

        if DeltaTable.isDeltaTable(batch_df.sparkSession, target_path):
            merge_delta_table(normalized_df, spec, target_path)
            print(f"    └─ MERGE xong {table_name}")
        else:
            created = initialize_delta_table(normalized_df, spec, target_path)
            if not created:
                print(f"  - {table_name}: chỉ có delete event, bỏ qua vì Delta table chưa tồn tại.")
                return
            print(f"    └─ Khởi tạo Delta table xong {table_name}")

        if args.skip_hive_sync:
            print(f"    └─ Bỏ qua Hive sync theo --skip-hive-sync")
        else:
            ensure_hive_table(batch_df.sparkSession, args.hive_db, table_name, target_path)
            print(f"    └─ Đồng bộ Hive Metastore xong {args.hive_db}.{table_name}")
    finally:
        normalized_df.unpersist()
