from typing import Optional

from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, length, lower, when
from pyspark.sql.types import DoubleType, StringType

from fabricks.context import SPARK
from fabricks.core.jobs.base import BaseJob


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


def decimal_to_double(df: DataFrame) -> DataFrame:
    cols = [name for name, dtype in df.dtypes if dtype.startswith("decimal") and not name.startswith("__")]
    for c in cols:
        df = df.withColumn(c, df[f"`{c}`"].cast(DoubleType()))
    return df


def timestamp_as_string(df: DataFrame) -> DataFrame:
    cols = [name for name, dtype in df.dtypes if dtype.startswith("timestamp")]
    for c in cols:
        df = df.withColumn(c, df[f"`{c}`"].cast(StringType()))
    return df


def boolean_as_string(df: DataFrame) -> DataFrame:
    cols = [name for name, dtype in df.dtypes if dtype.startswith("boolean")]
    for c in cols:
        df = df.withColumn(c, df[f"`{c}`"].cast(StringType()))
    return df


def assert_dfs_equal(df: DataFrame, df_expected: DataFrame):
    cols = df_expected.columns
    order_by = "id" if "id" in df.columns else "id"
    if "__valid_from" in df.columns:
        order_by = f"concat_ws('|', {order_by}, __valid_from, __valid_to)"

    def _transform(df_: DataFrame):
        df_ = df_.withColumn("order_by", expr(order_by)).orderBy("order_by").select(cols)
        df_ = df_.transform(decimal_to_double)
        df_ = df_.transform(timestamp_as_string)
        df_ = df_.transform(boolean_as_string)
        df_ = df_.transform(value_to_none)
        return df_

    print("<-- df -->\n")
    df = _transform(df)
    df.show()
    p_df = df.toPandas()

    print("<-- expected -->\n")
    df_expected = _transform(df_expected)
    df_expected.show()
    p_df_expected = df_expected.toPandas()

    assert_frame_equal(p_df, p_df_expected, check_dtype=False)


def compare_silver_to_expected(job: BaseJob, cdc: str, iter: int):
    if job.mode == "memory":
        df = SPARK.sql(f"select * from {job}")
    else:
        df = job.table.dataframe

    expected_df = SPARK.read.table(f"expected.silver_{cdc}_job{iter}")
    if job.topic in ["monarch", "memory", "regent"]:
        expected_df = expected_df.drop("__source")

    assert_dfs_equal(df, expected_df)


def compare_gold_to_expected(job: BaseJob, cdc: str, iter: int, where: Optional[str] = None):
    if job.mode == "memory":
        df = SPARK.sql(f"select * from {job}")
    else:
        df = job.table.dataframe

    if str(job) == "gold.scd1_memory":
        expected_df = SPARK.sql(
            f"""
        select
          id,
          name as monarch,
          doubleField as value,
          __is_current,
          __is_deleted
        from
          expected.silver_{cdc}_job{iter}
        """
        )
    else:
        expected_df = SPARK.read.table(f"expected.gold_{cdc}_job{iter}")

    if where:
        expected_df = expected_df.where(where)

    assert_dfs_equal(df, expected_df)


def get_last_error(job_id: str, status: str = "failed"):
    return (
        SPARK.sql(
            f"""
            select 
              l.exception.message as error, 
              l.timestamp 
            from 
              fabricks.logs l 
            where 
              true
              and l.job_id = '{job_id}' 
              and l.status = '{status}' 
            order by timestamp desc 
            limit 1
            """
        )
        .select("error")
        .collect()[0][0]
    )


def get_last_status(job_id: str):
    return (
        SPARK.sql(
            f"""
            select 
              l.status,
              l.timestamp 
            from 
              fabricks.logs l 
            where 
              true
              and l.job_id = '{job_id}' 
              and l.status in ('failed', 'done')
            order by timestamp desc 
            limit 1
            """
        )
        .select("status")
        .collect()[0][0]
    )
