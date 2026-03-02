from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.catalog import Column, Table

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.database import Database


class DbObject:
    def __init__(self, database: str, *levels: str, spark: Optional[SparkSession] = None):
        self.database = Database(database)
        self.levels = levels

        if spark is None:
            from fabricks.utils.spark import spark

            spark = spark

        assert spark is not None
        self.spark = spark

    @property
    def name(self) -> str:
        return "_".join(self.levels)

    @property
    def qualified_name(self) -> str:
        return f"{self.database.name}.{self.name}"

    @property
    def registered(self) -> bool:
        try:
            return self.spark.catalog.tableExists(self.qualified_name)
        except Exception:
            return False

    def get_spark_table(self) -> Table:
        return self.spark.catalog.getTable(self.qualified_name)

    def get_spark_columns(self) -> list[Column]:
        return self.spark.catalog.listColumns(self.qualified_name)

    @property
    def is_view(self) -> bool:
        try:
            table = self.get_spark_table()
            if table.tableType == "VIEW":
                return True

            return False

        except Exception:
            return False

    @property
    def is_table(self) -> bool:
        try:
            table = self.get_spark_table()
            if table.tableType == "VIEW":
                return False

            return True

        except Exception:
            return False

    def drop(self):
        if self.registered:
            if self.is_view:
                DEFAULT_LOGGER.warning("drop view from metastore", extra={"label": self})
                self.spark.sql(f"drop view if exists {self}")

            elif self.is_table:
                DEFAULT_LOGGER.warning("drop table from metastore", extra={"label": self})
                self.spark.sql(f"drop table if exists {self}")

            self._registered = False

        else:
            DEFAULT_LOGGER.debug("not found in metastore", extra={"label": self})

    def __str__(self):
        return self.qualified_name
