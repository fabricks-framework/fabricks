from databricks.sdk.runtime import dbutils, spark

from fabricks.context import BRONZE, GOLD, SECRET_SCOPE, SILVER
from fabricks.core.jobs.base.types import Bronzes, Golds, Silvers

# spark
SPARK = spark
DBUTILS = dbutils

# step
BRONZES = Bronzes
SILVERS = Silvers
GOLDS = Golds
STEPS = BRONZES + SILVERS + GOLDS


__all__ = [
    "BRONZE",
    "Bronzes",
    "BRONZES",
    "DBUTILS",
    "GOLD",
    "Golds",
    "GOLDS",
    "SECRET_SCOPE",
    "SILVER",
    "Silvers",
    "SILVERS",
    "SPARK",
    "STEPS",
]
