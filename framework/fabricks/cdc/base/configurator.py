from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Union

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc.base._types import AllowedSources
from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table
from fabricks.utils._types import DataFrameLike


class Configurator(ABC):
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
    def is_view(self):
        return self.table.is_view

    @property
    def registered(self):
        return self.table.registered

    @property
    def qualified_name(self):
        return f"{self.database}_{'_'.join(self.levels)}"

    @abstractmethod
    def get_query(self, src: AllowedSources, **kwargs) -> str: ...

    @abstractmethod
    def get_data(self, src: AllowedSources, **kwargs) -> DataFrame: ...

    @abstractmethod
    def create_table(
        self,
        src: AllowedSources,
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str]] = None,
        **kwargs,
    ): ...

    @abstractmethod
    def drop(self): ...

    @abstractmethod
    def create_or_replace_view(self, src: Union[Table, str], **kwargs): ...

    @property
    def allowed_input__columns(self) -> List[str]:
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
    def allowed_ouput_leading__columns(self) -> List[str]:
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
    def allowed_output_trailing__columns(self) -> List[str]:
        cols = [
            "__operation",
            "__metadata",
            "__rescued_data",
        ]

        if self.slowly_changing_dimension:
            cols.remove("__operation")

        return cols

    @property
    def __columns(self) -> List[str]:
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
            "__rescued_data",
        ]

    @property
    def slowly_changing_dimension(self) -> bool:
        return self.change_data_capture in ["scd1", "scd2"]

    def get_src(self, src: AllowedSources) -> DataFrame:
        if isinstance(src, DataFrameLike):
            df = src
        elif isinstance(src, Table):
            df = self.table.dataframe
        elif isinstance(src, str):
            df = self.spark.sql(src)
        else:
            raise ValueError(f"{src} not allowed")

        return df

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

        if backtick:
            return [f"`{c}`" for c in columns]
        else:
            return columns

    def sort_columns(self, columns: List[str]) -> List[str]:
        fields = [c for c in columns if not c.startswith("__")]

        leading = self.allowed_ouput_leading__columns
        trailing = self.allowed_output_trailing__columns

        __leading = [c for c in leading if c in columns]
        __trailing = [c for c in trailing if c in columns]

        return __leading + fields + __trailing

    def reorder_dataframe(self, df: DataFrame) -> DataFrame:
        columns = self.sort_columns(df.columns)
        columns = [f"`{c}`" for c in columns]
        return df.select(columns)

    @abstractmethod
    def optimize_table(self): ...

    @abstractmethod
    def update_schema(self, src: AllowedSources, **kwargs): ...

    @abstractmethod
    def get_differences_with_deltatable(self, src: AllowedSources, **kwargs): ...

    @abstractmethod
    def overwrite_schema(self, src: AllowedSources): ...

    def __str__(self):
        return f"{self.table.qualified_name}"
