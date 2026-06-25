from __future__ import annotations

from typing import Literal, Union

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from fabricks.metastore.table import Table

# Import from models for consistency

AllowedSources = Union[DataFrame, Table, str, StructType]
AllowedTemplates = Literal["filter", "merger", "query"]
