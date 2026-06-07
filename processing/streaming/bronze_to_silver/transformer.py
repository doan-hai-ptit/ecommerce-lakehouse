from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from .utils import bronze_payload_col, cast_json_value, default_value


def normalize_table_events(batch_df, table_name, spec):
    payload = bronze_payload_col()
    
    # Define Spark Schema for JSON parsing to read all fields as string first
    json_schema = StructType([
        StructField(column_name, StringType(), True) for column_name, _ in spec.columns
    ])
    
    # Parse the entire JSON payload exactly once
    parsed_df = batch_df.withColumn("parsed_payload", F.from_json(payload, json_schema))
    
    selected_columns = []
    for column_name, data_type in spec.columns:
        # Access the parsed field directly from the struct
        raw_value = F.col(f"parsed_payload.{column_name}")
        selected_columns.append(
            F.coalesce(cast_json_value(raw_value, data_type), default_value(data_type)).alias(column_name)
        )

    normalized = parsed_df.select(
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
