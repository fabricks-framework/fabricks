from __future__ import annotations

from typing import Literal, Union

from pyspark.sql import DataFrame

from fabricks.metastore.table import Table

AllowedChangeDataCaptures = Literal["nocdc", "scd1", "scd2"]
AllowedSources = Union[DataFrame, Table, str]
