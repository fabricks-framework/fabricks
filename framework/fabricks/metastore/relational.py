from typing import Optional

from databricks.sdk.runtime import dbutils as _dbutils
from databricks.sdk.runtime import spark as _spark
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import SparkSession

from fabricks.context.log import Logger
from fabricks.metastore.database import Database


class Relational:
    def __init__(self, database: str, *levels: str, spark: Optional[SparkSession] = None):
        self.database = Database(database)
        self.levels = levels
        if spark is None:
            spark = _spark
        assert spark is not None
        self.spark: SparkSession = spark
        self.dbutils = _dbutils

    @property
    def name(self) -> str:
        return "_".join(self.levels)

    @property
    def qualified_name(self) -> str:
        return f"{self.database.name}.{self.name}"

    def registered(self):
        try:
            df = self.spark.sql(f"show tables in {self.database}").where(f"tableName == '{self.name}'")
            return not df.isEmpty()
        # not found
        except AnalysisException:
            return False

    def is_view(self):
        try:
            df = self.spark.sql(f"show views in {self.database}").where(f"viewName == '{self.name}'")
            return not df.isEmpty()
        # not found
        except AnalysisException:
            return False

    def is_table(self):
        if self.is_view():
            return False
        else:
            return self.registered()

    def drop(self):
        if self.is_view():
            Logger.warning("drop view from metastore", extra={"job": self})
            self.spark.sql(f"drop view if exists {self}")
        elif self.is_table():
            Logger.warning("drop table from metastore", extra={"job": self})
            self.spark.sql(f"drop table if exists {self}")

    def __str__(self):
        return self.qualified_name
