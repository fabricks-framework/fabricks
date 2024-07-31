from typing import Optional

from databricks.sdk.runtime import spark as _spark
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from typing_extensions import deprecated

from fabricks.context import PATHS_STORAGE
from fabricks.context.log import Logger
from fabricks.metastore.utils import get_tables, get_views
from fabricks.utils.path import Path


class Database:
    def __init__(self, name: str, spark: Optional[SparkSession] = None):
        self.name = name
        storage = PATHS_STORAGE.get(self.name)
        assert storage is not None
        self.storage = storage
        if spark is None:
            spark = _spark
        assert spark is not None
        self.spark = spark

    @property
    @deprecated("use delta_path instead")
    def deltapath(self) -> Path:
        return self.storage.join("delta")

    @property
    def delta_path(self) -> Path:
        return self.storage.join("delta")

    def create(self):
        Logger.info("🌟 (create database)", extra={"step": self})
        self.spark.sql(f"create database if not exists {self.name};")

    def drop(self, rm: Optional[bool] = True):
        if self.exists():
            Logger.warning("💣 (drop database)", extra={"step": self})
            self.spark.sql(f"drop database if exists {self.name} cascade;")

        if rm:
            if self.deltapath.exists():
                Logger.debug("🧹 (remove delta files)", extra={"step": self})
                self.deltapath.rm()

    def exists(self) -> bool:
        try:
            self.spark.sql(f"show tables in {self.name}")
        # database not found
        except AnalysisException:
            return False
        return True

    def __str__(self):
        return self.name

    def get_tables(self) -> Optional[DataFrame]:
        try:
            df = get_tables(self.name)
            return df if not df.isEmpty() else None
        except AnalysisException:
            return None

    def get_views(self) -> Optional[DataFrame]:
        try:
            df = get_views(self.name)
            return df if not df.isEmpty() else None
        except AnalysisException:
            return None
