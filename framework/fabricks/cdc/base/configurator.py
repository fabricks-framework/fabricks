from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.dataframe import DataFrame as CDataFrame

from fabricks.context import SPARK
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table


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

    def is_view(self):
        return self.table.is_view()

    def registered(self):
        return self.table.registered()

    @abstractmethod
    def get_query(self, src: Union[DataFrame, Table, str], **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_data(self, src: Union[DataFrame, Table, str], **kwargs) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def create_table(
        self,
        src: Union[DataFrame, Table, str],
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str]] = None,
        **kwargs,
    ):
        raise NotImplementedError()

    @abstractmethod
    def drop(self):
        raise NotImplementedError()

    @abstractmethod
    def create_or_replace_view(self, src: Union[Table, str], **kwargs):
        raise NotImplementedError()

    @property
    def allowed_leading_columns(self):
        cols = [
            "__identity",
            "__source",
            "__key",
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
    def allowed_trailing_columns(self):
        cols = [
            "__operation",
            "__metadata",
            "__hash",
            "__rescued_data",
        ]

        if self.change_data_capture == "scd1":
            cols.remove("__operation")
        elif self.change_data_capture == "scd2":
            cols.remove("__operation")

        return cols

    @property
    def slowly_changing_dimension(self) -> bool:
        return self.change_data_capture in ["scd1", "scd2"]

    def get_src(self, src: Union[DataFrame, Table, str]) -> DataFrame:
        if isinstance(src, (DataFrame, CDataFrame)):
            df = src
        elif isinstance(src, Table):
            df = self.table.dataframe
        elif isinstance(src, str):
            df = self.spark.sql(src)
        else:
            raise ValueError(f"{src} not allowed")

        return df

    def get_columns(self, src: Union[DataFrame, Table, str], backtick: Optional[bool] = True) -> List[str]:
        if backtick:
            backtick = True

        df = self.get_src(src=src)
        columns = df.columns

        if backtick:
            return [f"`{c}`" for c in columns]
        else:
            return columns

    def reorder_columns(self, df: DataFrame) -> DataFrame:
        fields = [f"`{c}`" for c in df.columns if not c.startswith("__")]

        leading = self.allowed_leading_columns
        trailing = self.allowed_trailing_columns
        if (
            "__key" not in df.columns and "__hash" in df.columns
        ):  # move __hash to the front of the table to ensure statistics are present
            leading = ["__hash" if c == "__key" else c for c in leading]
            trailing = [c for c in trailing if c != "__hash"]

        __leading = [c for c in leading if c in df.columns]
        __trailing = [c for c in trailing if c in df.columns]

        columns = __leading + fields + __trailing

        return df.select(columns)

    @abstractmethod
    def optimize_table(self):
        raise NotImplementedError()

    @abstractmethod
    def update_schema(self, src: Union[DataFrame, Table, str], **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_differences_with_deltatable(self, src: Union[DataFrame, Table, str], **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def overwrite_schema(self, src: Union[DataFrame, Table, str]):
        raise NotImplementedError()

    def __str__(self):
        return f"{self.table.qualified_name}"
