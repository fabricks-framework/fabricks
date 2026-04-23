"""Spark-dependent utilities."""

from fabricks.utils.spark.core import DATABRICKS_LOCALMODE, dbutils, display, get_dbutils, get_spark, spark
from fabricks.utils.spark.paths import FileSharePath, resolve_fileshare_path

__all__ = [
    "DATABRICKS_LOCALMODE",
    "FileSharePath",
    "dbutils",
    "display",
    "get_dbutils",
    "get_spark",
    "resolve_fileshare_path",
    "spark",
]
