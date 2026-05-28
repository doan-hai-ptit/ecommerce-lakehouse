from pyspark.sql import Window
from pyspark.sql import functions as F
from .utils import bronze_payload_col, json_scalar, cast_json_value, default_value


def normalize_table_events(batch_df, table_name, spec):
    payload = bronze_payload_col()
    selected_columns = []

    for column_name, data_type in spec.columns:
        raw_value = json_scalar(payload, column_name)
        selected_columns.append(
            F.coalesce(cast_json_value(raw_value, data_type), default_value(data_type)).alias(column_name)
        )

    normalized = batch_df.select(
        *selected_columns,
        F.col("event_date").cast("date").alias("event_date"),
        F.coalesce(F.col("debezium_op"), F.lit("r")).alias("_change_op"),
        F.col("kafka_timestamp").alias("_kafka_timestamp"),
        F.col("offset").cast("long").alias("_kafka_offset"),
    )

    key_is_present = None
    for key in spec.primary_keys:
        key_condition = F.col(key).isNotNull()
        key_is_present = key_condition if key_is_present is None else key_is_present & key_condition

    normalized = normalized.where(key_is_present)

    order_cols = [
        F.col("_kafka_timestamp").desc_nulls_last(),
        F.col("_kafka_offset").desc_nulls_last(),
    ]
    window = Window.partitionBy(*[F.col(key) for key in spec.primary_keys]).orderBy(*order_cols)

    return normalized.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")
