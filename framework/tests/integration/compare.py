from typing import Literal, Optional

from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from fabricks.context import SPARK
from fabricks.core.jobs.base import BaseJob
from fabricks.utils.dataframe import boolean_as_string, decimal_to_double, timestamp_as_string, value_to_none

__COLUMNS = ["__is_current", "__is_deleted", "__valid_from", "__valid_to", "__source"]


def assert_dfs_equal(df: DataFrame, df_expected: DataFrame, soft_delete: bool = True):
    cols = df_expected.columns
    cols = [c for c in cols if not c.startswith("__") or c in __COLUMNS]
    scd2 = "__valid_from" in cols and "__valid_to" in cols

    if not soft_delete:
        if scd2:
            cols = [c for c in cols if c not in ["__is_deleted"]]  # __is_current is always present in SCD2
        else:
            df_expected = df_expected.where("__is_current")
            cols = [c for c in cols if c not in ["__is_deleted", "__is_current"]]

    priority = ["id"]

    if scd2:
        priority += ["__valid_from", "__valid_to"]

    sort_cols = [c for c in priority if c in cols] + [c for c in cols if c not in priority]
    order_by = f"concat_ws('|', {', '.join(sort_cols)})"

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


def compare_to_expected(
    job: BaseJob,
    expected: Literal["scd0", "scd1", "scd2", "latest", "append"],
    iter: int,
    reloaded: bool = False,
):
    expand = job.expand
    expected_df = SPARK.sql(f"select * from expected.{expand}_{expected}_job{iter}")
    df = SPARK.sql(f"select * from {job}")

    if expand in ["bronze", "silver"]:
        if job.topic in ["monarch", "memory", "regent"]:
            expected_df = expected_df.drop("__source")
    else:
        if job.change_data_capture == "scd1" and (job.mode == "complete" or reloaded):
            expected_df = expected_df.where("__is_current")

    assert_dfs_equal(df, expected_df)


def compare_silver_to_expected(job: BaseJob, cdc: Literal["scd1", "scd2"], iter: int):
    if job.mode == "memory":
        df = SPARK.sql(f"select * from {job}")
    else:
        df = job.table.dataframe

    expected_df = SPARK.read.table(f"expected.silver_{cdc}_job{iter}")

    if job.topic in ["monarch", "memory", "regent"]:
        expected_df = expected_df.drop("__source")

    assert_dfs_equal(df, expected_df)


def compare_gold_to_expected(job: BaseJob, cdc: Literal["scd1", "scd2"], iter: int, where: Optional[str] = None):
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
