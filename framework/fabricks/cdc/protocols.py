from __future__ import annotations

from typing import List, Optional, Protocol, Union

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc.config import AllowedSources
from fabricks.metastore.table import Table


class CDCProtocol(Protocol):
    @property
    def spark(self) -> SparkSession: ...

    @property
    def table(self) -> Table: ...

    @property
    def change_data_capture(self) -> str: ...

    @property
    def is_view(self) -> bool: ...

    @property
    def qualified_name(self) -> str: ...

    @property
    def slowly_changing_dimension(self) -> bool: ...

    def get_data(self, src: AllowedSources, **kwargs) -> DataFrame: ...

    def get_query(self, src: AllowedSources, fix: Optional[bool] = True, **kwargs) -> str: ...

    def fix_sql(self, sql: str) -> str: ...

    def get_columns(
        self,
        src: AllowedSources,
        backtick: Optional[bool] = True,
        sort: Optional[bool] = True,
        check: Optional[bool] = True,
    ) -> List[str]: ...

    def sort_columns(self, columns: List[str]) -> List[str]: ...

    def reorder_dataframe(self, df: DataFrame, extra__columns: Optional[List[str]] = None) -> DataFrame: ...

    def has_data(self, src: AllowedSources, **kwargs) -> bool: ...

    def create_table(self, src: AllowedSources, **kwargs): ...

    def create_or_replace_view(self, src: Union[AllowedSources, str], schema_evolution: bool = True, **kwargs): ...
