from typing import Optional

from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, Row, SparkSession
from typing_extensions import deprecated

from fabricks.context import PATHS_STORAGE, SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.utils import get_tables, get_views
from fabricks.utils.helpers import run_in_parallel
from fabricks.utils.path import FileSharePath


class Database:
    def __init__(self, name: str, spark: Optional[SparkSession] = None):
        self.name = name
        storage = PATHS_STORAGE.get(self.name)
        assert storage is not None
        self.storage = storage

        if spark is None:
            spark = SPARK

        assert spark is not None
        self.spark = spark

    @property
    @deprecated("use delta_path instead")
    def deltapath(self) -> FileSharePath:
        return self.storage.joinpath("delta")

    @property
    def delta_path(self) -> FileSharePath:
        return self.storage.joinpath("delta")

    def create(self):
        DEFAULT_LOGGER.info("create database", extra={"label": self})
        self.spark.sql(f"create database if not exists {self.name};")

    def drop(self, rm: Optional[bool] = True, one_by_one: Optional[bool] = True):
        if one_by_one:
            self.drop_one_by_one()

        if self.exists():
            DEFAULT_LOGGER.warning("drop database", extra={"label": self})
            self.spark.sql(f"drop database if exists {self.name} cascade;")

        if rm:
            if self.delta_path.exists():
                DEFAULT_LOGGER.debug("remove delta files", extra={"label": self})
                self.delta_path.rm()

    def drop_one_by_one(self):
        tables = self.get_tables()
        views = self.get_views()

        def _drop_view(row: Row):
            self.spark.sql(f"drop view if exists {row['view']};")

        def _drop_table(row: Row):
            self.spark.sql(f"drop table if exists {row['table']};")

        run_in_parallel(_drop_table, tables)
        run_in_parallel(_drop_view, views)

    def exists(self) -> bool:
        try:
            self.spark.sql(f"show tables in {self.name}")
            return True

        # database not found
        except AnalysisException:
            return False

    def __str__(self):
        return self.name

    def get_tables(self) -> DataFrame:
        return get_tables(self.name)

    def get_views(self) -> DataFrame:
        return get_views(self.name)
