import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame


def _convert_null_like_values(col: Column) -> Column:
    col_str = col.cast("string")

    return (
        F.when(F.length(col_str) == 0, None)
        .when(F.lower(col_str) == "none", None)
        .when(F.lower(col_str) == "null", None)
        .when(F.lower(col_str) == "blank", None)
        .when(F.lower(col_str) == "(none)", None)
        .when(F.lower(col_str) == "(null)", None)
        .when(F.lower(col_str) == "(blank)", None)
        .otherwise(col)
    )


def value_to_none(df: DataFrame) -> DataFrame:
    cols_to_transform = {name for name, dtype in df.dtypes if not name.startswith("__")}

    return df.select(
        [
            _convert_null_like_values(df[f"`{c}`"]).alias(c) if c in cols_to_transform else df[f"`{c}`"]
            for c in df.columns
        ]
    )


def decimal_to_float(df: DataFrame) -> DataFrame:
    decimal_cols = {name for name, dtype in df.dtypes if dtype.startswith("decimal") and not name.startswith("__")}

    return df.select(
        [df[f"`{c}`"].cast(T.FloatType()).alias(c) if c in decimal_cols else df[f"`{c}`"] for c in df.columns]
    )


def decimal_to_double(df: DataFrame) -> DataFrame:
    decimal_cols = {name for name, dtype in df.dtypes if dtype.startswith("decimal") and not name.startswith("__")}

    return df.select(
        [df[f"`{c}`"].cast(T.DoubleType()).alias(c) if c in decimal_cols else df[f"`{c}`"] for c in df.columns]
    )


def tinyint_to_int(df: DataFrame) -> DataFrame:
    tinyint_cols = {name for name, dtype in df.dtypes if dtype.startswith("tinyint") and not name.startswith("__")}

    return df.select(
        [df[f"`{c}`"].cast(T.IntegerType()).alias(c) if c in tinyint_cols else df[f"`{c}`"] for c in df.columns]
    )


def trim(df: DataFrame) -> DataFrame:
    string_cols = {name for name, dtype in df.dtypes if dtype.startswith("string") and not name.startswith("__")}

    return df.select([F.trim(df[f"`{c}`"]).alias(c) if c in string_cols else df[f"`{c}`"] for c in df.columns])


def timestamp_as_string(df: DataFrame) -> DataFrame:
    timestamp_cols = {name for name, dtype in df.dtypes if dtype.startswith("timestamp")}

    return df.select(
        [df[f"`{c}`"].cast(T.StringType()).alias(c) if c in timestamp_cols else df[f"`{c}`"] for c in df.columns]
    )


def boolean_as_string(df: DataFrame) -> DataFrame:
    boolean_cols = {name for name, dtype in df.dtypes if dtype.startswith("boolean")}

    return df.select(
        [df[f"`{c}`"].cast(T.StringType()).alias(c) if c in boolean_cols else df[f"`{c}`"] for c in df.columns]
    )


def clean(df: DataFrame) -> DataFrame:
    """
    Cleans the given DataFrame by performing the following operations:
    1. Trims whitespace from all string columns.
    2. Converts empty strings and null-like values to None.
    3. Converts decimal values to double.

    Args:
        df: The PySpark DataFrame to be cleaned.

    Returns:
        The cleaned PySpark DataFrame.
    """
    df = trim(df)
    df = value_to_none(df)
    df = decimal_to_double(df)
    return df
