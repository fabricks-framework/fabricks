from __future__ import annotations

from typing import Union

from pyspark.sql import DataFrame

from fabricks.metastore.table import Table

# Import from models for consistency

AllowedSources = Union[DataFrame, Table, str]
