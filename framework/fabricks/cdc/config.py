from __future__ import annotations

from typing import List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table
from fabricks.utils._types import DataFrameLike
from fabricks.utils.helpers import backticks

AllowedSources = Union[DataFrame, Table, str, StructType]


class CDCConfig:
    """Resolved configuration substrate for a CDC instance.

    Holds identity (database, levels, change_data_capture), the Table
    reference, the Spark session, column-ordering rules, and the
    utility helpers (get_src, has_data, get_columns, sort_columns,
    reorder_dataframe) that every delegate needs.

    No behaviour — no DDL, no queries, no merges. Constructable in a
    test directly from primitive args.
    """

    def __init__(
        self,
        database: str,
        *levels: str,
        change_data_capture: str,
        spark: Optional[SparkSession] = None,
    ):
        if spark is None:
            spark = SPARK
        assert spark is not None
        self.spark: SparkSession = spark

        self.database = Database(database)
        self.levels = levels
        self.change_data_capture = change_data_capture
        self.table = Table(self.database.name, *self.levels, spark=self.spark)

    @property
    def is_view(self) -> bool:
        return self.table.is_view

    @property
    def registered(self) -> bool:
        return self.table.registered

    @property
    def qualified_name(self) -> str:
        return f"{self.database}_{'_'.join(self.levels)}"

    @property
    def slowly_changing_dimension(self) -> bool:
        return self.change_data_capture in ["scd0", "scd1", "scd2"]

    @property
    def __columns(self) -> List[str]:
        return [
            "__identity",
            "__source",
            "__key",
            "__hash",
            "__timestamp",
            "__valid_from",
            "__valid_to",
            "__is_current",
            "__is_deleted",
            "__operation",
            "__metadata",
            "__last_updated",
            "__rescued_data",
        ]

    @property
    def allowed_input__columns(self) -> List[str]:
        scd_only = {"__valid_from", "__valid_to", "__is_current", "__is_deleted"}
        excluded = scd_only if self.slowly_changing_dimension else set()
        return [c for c in self.__columns if c not in excluded]

    @property
    def allowed_ouput_leading__columns(self) -> List[str]:
        base = [
            "__identity",
            "__source",
            "__key",
            "__hash",
            "__timestamp",
            "__valid_from",
            "__valid_to",
            "__is_current",
            "__is_deleted",
        ]
        if self.change_data_capture == "scd1":
            return [c for c in base if c not in {"__valid_from", "__valid_to"}]
        if self.change_data_capture == "scd2":
            return [c for c in base if c != "__timestamp"]
        return base

    @property
    def allowed_output_trailing__columns(self) -> List[str]:
        base = ["__operation", "__metadata", "__last_updated", "__rescued_data"]
        if self.slowly_changing_dimension:
            return [c for c in base if c != "__operation"]
        return base

    def get_src(self, src: AllowedSources) -> DataFrameLike:
        if isinstance(src, DataFrameLike):
            return src
        elif isinstance(src, Table):
            return self.table.dataframe
        elif isinstance(src, str):
            return self.spark.sql(src)
        elif isinstance(src, StructType):
            return self.spark.createDataFrame([], schema=src)
        else:
            raise ValueError(f"{src} not allowed")

    def has_data(self, src: AllowedSources, **kwargs) -> bool:
        DEFAULT_LOGGER.debug("check if has data", extra={"label": self})
        df = self.get_src(src=src)
        return not df.isEmpty()

    def get_columns(
        self,
        src: AllowedSources,
        backtick: Optional[bool] = True,
        sort: Optional[bool] = True,
        check: Optional[bool] = True,
    ) -> List[str]:
        df = self.get_src(src=src)
        columns = df.columns

        if check:
            for c in columns:
                if c.startswith("__") and c in self.__columns:
                    assert c in self.allowed_input__columns, f"{c} is not allowed"

        if sort:
            columns = self.sort_columns(columns)

        return backticks(columns) if backtick else columns

    def sort_columns(self, columns: List[str]) -> List[str]:
        fields = [c for c in columns if not c.startswith("__")]

        leading = self.allowed_ouput_leading__columns
        trailing = self.allowed_output_trailing__columns

        for c in columns:
            if c.startswith("__cluster"):
                leading.append(c)
            elif c.startswith("__partition"):
                trailing.append(c)

        __leading = [c for c in leading if c in columns]
        __trailing = [c for c in trailing if c in columns]

        return __leading + fields + __trailing

    def reorder_dataframe(self, df: DataFrame, extra__columns: Optional[List[str]] = None) -> DataFrame:
        columns = self.sort_columns(df.columns)
        if extra__columns:
            extra__columns = [c for c in extra__columns if c in df.columns]
            columns += extra__columns

        columns = backticks(columns)
        return df.select(columns)

    def __str__(self) -> str:
        return f"{self.table.qualified_name}"
