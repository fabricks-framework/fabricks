from pyspark.sql import DataFrame
from pyspark.sql.functions import length, lower
from pyspark.sql.functions import trim as _trim
from pyspark.sql.functions import when
from pyspark.sql.types import DoubleType, FloatType, IntegerType


def value_to_none(df: DataFrame) -> DataFrame:
    cols = [name for name, dtype in df.dtypes if not name.startswith("__")]
    for c in cols:
        df = df.withColumn(
            c,
            when(length(df[f"`{c}`"].cast("string")) == 0, None)
            .when(lower(df[f"`{c}`"].cast("string")) == "none", None)
            .when(lower(df[f"`{c}`"].cast("string")) == "null", None)
            .when(lower(df[f"`{c}`"].cast("string")) == "blank", None)
            .when(lower(df[f"`{c}`"].cast("string")) == "(none)", None)
            .when(lower(df[f"`{c}`"].cast("string")) == "(null)", None)
            .when(lower(df[f"`{c}`"].cast("string")) == "(blank)", None)
            .otherwise(df[f"`{c}`"]),
        )
    return df


def decimal_to_float(df: DataFrame) -> DataFrame:
    cols = [name for name, dtype in df.dtypes if dtype.startswith("decimal") and not name.startswith("__")]
    for c in cols:
        df = df.withColumn(c, df[f"`{c}`"].cast(FloatType()))
    return df


def decimal_to_double(df: DataFrame) -> DataFrame:
    cols = [name for name, dtype in df.dtypes if dtype.startswith("decimal") and not name.startswith("__")]
    for c in cols:
        df = df.withColumn(c, df[f"`{c}`"].cast(DoubleType()))
    return df


def tinyint_to_int(df: DataFrame) -> DataFrame:
    cols = [name for name, dtype in df.dtypes if dtype.startswith("tinyint") and not name.startswith("__")]
    for c in cols:
        df = df.withColumn(c, df[f"`{c}`"].cast(IntegerType()))
    return df


def trim(df: DataFrame) -> DataFrame:
    cols = [name for name, dtype in df.dtypes if dtype.startswith("string") and not name.startswith("__")]
    for c in cols:
        df = df.withColumn(c, _trim(df[f"`{c}`"]))
    return df


def clean(df: DataFrame) -> DataFrame:
    """
    Cleans the given DataFrame by performing the following operations:
    1. Trims whitespace from all string columns.
    2. Converts empty strings to None.
    3. Converts decimal values to double.

    Args:
        df (pandas.DataFrame): The DataFrame to be cleaned.

    Returns:
        pandas.DataFrame: The cleaned DataFrame.
    """
    df = trim(df)
    df = value_to_none(df)
    df = decimal_to_double(df)
    return df
