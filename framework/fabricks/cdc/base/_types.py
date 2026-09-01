from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from fabricks.metastore.table import Table

# Import from models for consistency

AllowedSources = DataFrame | Table | str | StructType
