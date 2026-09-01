from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from fabricks.cdc.base._types import AllowedSources
from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table
from fabricks.utils._types import DataFrameLike
from fabricks.utils.helpers import backticks


class Configurator(ABC):
    def __init__(
        self, database: str, *levels: str, change_data_capture: str, spark: SparkSession | None = None
    ) -> None:
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

    @abstractmethod
    def get_query(self, src: AllowedSources, **kwargs: Any) -> str: ...  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline

    @abstractmethod
    def get_data(self, src: AllowedSources, **kwargs: Any) -> DataFrame: ...  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline

    @abstractmethod
    def create_table(
        self,
        src: AllowedSources,
        partitioning: bool | None = False,
        partition_by: list[str] | str | None = None,
        identity: bool | None = False,
        liquid_clustering: bool | None = False,
        cluster_by: list[str] | str | None = None,
        properties: dict[str, str | bool | int] | None = None,
        masks: dict[str, str] | None = None,
        primary_key: dict[str, Any] | None = None,
        foreign_keys: dict[str, Any] | None = None,
        generated_columns: dict[str, str] | None = None,
        comments: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
    ) -> None: ...

    @abstractmethod
    def drop(self) -> None: ...

    @abstractmethod
    def create_or_replace_view(self, src: Table | str, **kwargs: Any) -> None: ...  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline

    @property
    def allowed_input__columns(self) -> list[str]:
        cols = self.__columns

        if self.slowly_changing_dimension:
            if "__valid_from" in cols:
                cols.remove("__valid_from")
            if "__valid_to" in cols:
                cols.remove("__valid_to")
            if "__is_current" in cols:
                cols.remove("__is_current")
            if "__is_deleted" in cols:
                cols.remove("__is_deleted")

        return cols

    @property
    def allowed_ouput_leading__columns(self) -> list[str]:
        cols = [
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
            cols.remove("__valid_from")
            cols.remove("__valid_to")
        elif self.change_data_capture == "scd2":
            cols.remove("__timestamp")

        return cols

    @property
    def allowed_output_trailing__columns(self) -> list[str]:
        cols = ["__operation", "__metadata", "__last_updated", "__rescued_data"]

        if self.slowly_changing_dimension:
            cols.remove("__operation")

        return cols

    @property
    def __columns(self) -> list[str]:
        return [
            # Leading
            "__identity",
            "__source",
            "__key",
            "__hash",
            "__timestamp",
            "__valid_from",
            "__valid_to",
            "__is_current",
            "__is_deleted",
            # Trailing
            "__operation",
            "__metadata",
            "__last_updated",
            "__rescued_data",
        ]

    @property
    def slowly_changing_dimension(self) -> bool:
        return self.change_data_capture in ["scd0", "scd1", "scd2"]

    def get_src(self, src: AllowedSources) -> DataFrameLike:
        if isinstance(src, DataFrameLike):
            df = src
        elif isinstance(src, Table):
            df = self.table.dataframe
        elif isinstance(src, str):
            df = self.spark.sql(src)
        elif isinstance(src, StructType):
            df = self.spark.createDataFrame([], schema=src)
        else:
            raise ValueError(f"{src} not allowed")

        return df

    def has_data(self, src: AllowedSources, **_kwargs: Any) -> bool:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        DEFAULT_LOGGER.debug("check if has data", extra={"label": self})
        df = self.get_src(src=src)
        return not df.isEmpty()

    def get_columns(
        self, src: AllowedSources, backtick: bool | None = True, sort: bool | None = True, check: bool | None = True
    ) -> list[str]:
        if backtick:
            backtick = True

        df = self.get_src(src=src)
        columns = df.columns

        if check:
            for c in columns:
                # avoid duplicate column issue in merge
                if c.startswith("__") and c in self.__columns:
                    assert c in self.allowed_input__columns, f"{c} is not allowed"

        if sort:
            columns = self.sort_columns(columns)

        return backticks(columns) if backtick else columns

    def sort_columns(self, columns: list[str]) -> list[str]:
        fields = [c for c in columns if not c.startswith("__")]

        leading = self.allowed_ouput_leading__columns
        trailing = self.allowed_output_trailing__columns

        for c in columns:
            if c.startswith("__cluster"):
                leading.append(c)  # need to be at the front to have statistics for clustering
            elif c.startswith("__partition"):
                trailing.append(c)  # need to be at the end to avoid issues with generated columns

        __leading = [c for c in leading if c in columns]
        __trailing = [c for c in trailing if c in columns]

        return __leading + fields + __trailing

    def reorder_dataframe(self, df: DataFrame, extra__columns: list[str] | None = None) -> DataFrame:
        columns = self.sort_columns(df.columns)
        if extra__columns:
            extra__columns = [c for c in extra__columns if c in df.columns]
            columns += extra__columns

        columns = backticks(columns)
        return df.select(columns)

    @abstractmethod
    def optimize_table(self) -> None: ...

    @abstractmethod
    def update_schema(self, src: AllowedSources, **kwargs: Any) -> None: ...  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline

    @abstractmethod
    def get_differences_with_deltatable(self, src: AllowedSources, **kwargs: Any) -> DataFrame: ...  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline

    @abstractmethod
    def overwrite_schema(self, src: AllowedSources) -> None: ...

    def __str__(self) -> str:
        return f"{self.table.qualified_name}"
