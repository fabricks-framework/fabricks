from dataclasses import dataclass
from typing import Literal

from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from fabricks.cdc import SCD1, SCD2, NoCDC
from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base import BaseJob
from fabricks.models.cdc import CdcContext
from fabricks.utils.dataframe import boolean_as_string, decimal_to_double, timestamp_as_string, value_to_none

CDC = {"scd1": SCD1, "scd2": SCD2, "nocdc": NoCDC}
__COLUMNS = ["__is_current", "__is_deleted", "__valid_from", "__valid_to", "__source"]


@dataclass
class ExpectedSpec:
    expand: str  # bronze | silver | gold -- selects expected.{expand}_{variant}_job{iter}
    variant: str  # scd0 | scd1 | scd2 | latest | append | nocdc -- selects expected.{expand}_{variant}_job{iter}
    iter: int
    job: BaseJob | None = None
    obj: str | None = None
    reloaded: bool = False  # if table is reloaded, history is not rebuilt
    topic: str | None = None  # real topic name, e.g. "king_and_queen" -- drives source-relevance
    expected_query: str | None = None  # overrides the default `expected.{expand}_{variant}_job{iter}` lookup

    @property
    def _keep_source(self) -> bool:
        # __source only reflects real data when merging multiple topics -- currently only king_and_queen
        if self.job is not None:
            return self.job.topic == "king_and_queen"

        if self.obj is not None:
            return "king_and_queen" in self.obj

        return self.topic == "king_and_queen"

    @property
    def _where_current(self) -> bool:
        if self.job is not None:
            return self.job.change_data_capture == "scd1" and (self.job.mode == "complete" or self.reloaded)

        return self.variant == "scd1" and self.reloaded

    def assert_equal(self, df: DataFrame, soft_delete: bool = True):
        query = self.expected_query or f"select * from expected.{self.expand}_{self.variant}_job{self.iter}"
        expected_df = SPARK.sql(query)

        if self.expand == "gold" and self._where_current:
            expected_df = expected_df.where("__is_current")

        assert_dfs_equal(df, expected_df, soft_delete=soft_delete, keep_source=self._keep_source)


def assert_dfs_equal(df: DataFrame, df_expected: DataFrame, soft_delete: bool = True, keep_source: bool = True):
    cols = df_expected.columns
    cols = [c for c in cols if not c.startswith("__") or c in __COLUMNS]
    scd2 = "__valid_from" in cols and "__valid_to" in cols

    if not soft_delete:
        if scd2:
            cols = [c for c in cols if c not in ["__is_deleted"]]  # __is_current is always present in SCD2
        else:
            df_expected = df_expected.where("__is_current")
            cols = [c for c in cols if c not in ["__is_deleted", "__is_current"]]

    if not keep_source:
        cols = [c for c in cols if c != "__source"]

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


def compare_object_to_expected(
    expand: Literal["bronze", "silver", "gold"],
    obj: str,
    expected: Literal["scd0", "scd1", "scd2", "latest", "append"],
    iter: int,
    reloaded: bool = False,
):
    spec = ExpectedSpec(expand=expand, variant=expected, iter=iter, obj=obj, reloaded=reloaded)
    df = SPARK.sql(f"select * from {obj}")
    spec.assert_equal(df)


def compare_job_to_expected(
    job: BaseJob,
    expected: Literal["scd0", "scd1", "scd2", "latest", "append"],
    iter: int,
    reloaded: bool = False,
):
    spec = ExpectedSpec(expand=job.expand, variant=expected, iter=iter, job=job, reloaded=reloaded)
    df = SPARK.sql(f"select * from {job}")
    spec.assert_equal(df)


def compare_cdc_to_expected(
    topic: Literal["monarch", "king_and_queen", "prince", "princesses", "duke"],
    cdc: Literal["scd1", "scd2", "nocdc"],
    iter: int | list[int] = 1,
    mode: Literal["latest", "append", "update", "overwrite"] = "update",
    soft_delete: bool = False,
):
    if mode == "latest":
        assert cdc == "nocdc"  # mandatory for latest
        assert topic == "duke"  # mandatory for latest as duke force reload in __operation
    elif mode == "append":
        assert cdc == "nocdc"  # mandatory for append

    if isinstance(iter, int):
        iter = [iter]

    if mode == "append":
        # append relies on non-cumulative batch tables, so gaps must be filled to not lose data
        iter = list(range(min(iter), max(iter) + 1))

    context = CdcContext(keys=["id"], schema_drift=True, soft_delete=soft_delete)

    if mode == "latest":
        context.slice = "latest"
    elif cdc == "scd2":
        context.correct_valid_from = True

    iter_str = [str(i) for i in iter]
    last_iter = iter[-1]
    tgt = CDC[cdc]("test", f"{topic}_{cdc}_{'_'.join(iter_str)}")
    variant = "latest" if mode == "latest" else "append" if mode == "append" else cdc
    DEFAULT_LOGGER.info(f"comparing to {variant} job {last_iter}")
    x = 0

    for i in iter_str:
        if topic == "king_and_queen":
            view_1 = f"input.king_job{i}"
            view_2 = f"input.queen_job{i}"

            if mode == "append":
                view_1 = view_1 + "_batch"
                view_2 = view_2 + "_batch"

            query = f"select *, 'king' as __source from {view_1} union all select *, 'queen' as __source from {view_2}"
        else:
            view = f"input.{topic}_job{i}"

            if mode == "append":
                view = view + "_batch"

            query = f"select * from {view}"

        if x == 0:
            tgt.drop()
            tgt.create_table(query, context=context)

        if mode == "append":
            tgt.append(query, context=context)
        elif mode == "overwrite":
            tgt.overwrite(query, context=context)
        else:
            tgt.update(query, context=context)

        x += 1

    spec = ExpectedSpec(
        expand="silver",
        variant=variant,
        iter=last_iter,
        topic=topic,
        # append relies on non-cumulative batch tables, so the raw cumulative input *is* the expectation
        expected_query=f"select * from input.{topic}_job{last_iter}" if mode == "append" else None,
    )
    df = tgt.table.dataframe
    spec.assert_equal(df, soft_delete=soft_delete)
