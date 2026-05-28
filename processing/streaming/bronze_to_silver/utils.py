import re
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


def sql_identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return f"`{name}`"


def table_path(silver_base, table_name):
    return f"{silver_base.rstrip('/')}/{table_name}"


def bronze_payload_col():
    return F.when(F.col("debezium_op") == F.lit("d"), F.col("payload_before")).otherwise(
        F.col("payload_after")
    )


def json_scalar(payload, column_name):
    return F.get_json_object(payload, f"$.{column_name}")


def cast_json_value(raw_value, data_type):
    if isinstance(data_type, StringType):
        value = F.trim(raw_value.cast("string"))
        return F.when(F.length(value) > 0, value)

    if isinstance(data_type, BooleanType):
        return raw_value.cast("boolean")

    if isinstance(data_type, IntegerType):
        return raw_value.cast("int")

    if isinstance(data_type, LongType):
        return raw_value.cast("long")

    if isinstance(data_type, DecimalType):
        return raw_value.cast(data_type)

    if isinstance(data_type, DateType):
        text_value = raw_value.cast("string")
        days_from_epoch = text_value.cast("int")
        return F.when(
            text_value.rlike(r"^-?\d+$"),
            F.date_add(F.lit("1970-01-01").cast("date"), days_from_epoch),
        ).otherwise(F.to_date(text_value))

    if isinstance(data_type, TimestampType):
        text_value = raw_value.cast("string")
        numeric_value = text_value.cast("double")
        numeric_timestamp = (
            F.when(
                F.abs(numeric_value) >= F.lit(1000000000000000),
                F.to_timestamp(F.from_unixtime(numeric_value / F.lit(1000000))),
            )
            .when(
                F.abs(numeric_value) >= F.lit(1000000000000),
                F.to_timestamp(F.from_unixtime(numeric_value / F.lit(1000))),
            )
            .otherwise(F.to_timestamp(F.from_unixtime(numeric_value)))
        )
        normalized_text = F.regexp_replace(text_value, "T", " ")
        normalized_text = F.regexp_replace(normalized_text, "Z$", "")
        return F.when(text_value.rlike(r"^-?\d+(\.\d+)?$"), numeric_timestamp).otherwise(
            F.coalesce(F.to_timestamp(text_value), F.to_timestamp(normalized_text))
        )

    return raw_value


def default_value(data_type):
    if isinstance(data_type, BooleanType):
        return F.lit(None).cast("boolean")
    if isinstance(data_type, IntegerType):
        return F.lit(None).cast("int")
    if isinstance(data_type, LongType):
        return F.lit(None).cast("long")
    if isinstance(data_type, DecimalType):
        return F.lit(None).cast(data_type)
    if isinstance(data_type, DateType):
        return F.lit(None).cast("date")
    if isinstance(data_type, TimestampType):
        return F.lit(None).cast("timestamp")
    return F.lit(None).cast("string")


def output_columns(spec):
    return [column_name for column_name, _ in spec.columns] + ["event_date"]


def non_delete_rows(df, spec):
    return df.where(F.col("_change_op") != F.lit("d")).select(*output_columns(spec))


def has_rows(df):
    return len(df.take(1)) > 0
