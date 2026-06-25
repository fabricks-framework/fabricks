from __future__ import annotations

from typing import List, Optional, Protocol, Tuple

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc.mixins._types import AllowedSources, AllowedTemplates
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table


class CdcProtocol(Protocol):
    spark: SparkSession
    table: Table
    database: Database
    levels: Tuple[str, ...]
    change_data_capture: str

    @property
    def slowly_changing_dimension(self) -> bool: ...

    @property
    def is_view(self) -> bool: ...

    @property
    def qualified_name(self) -> str: ...

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

    def get_data(self, src: AllowedSources, **kwargs) -> DataFrame: ...

    def get_query(self, src: AllowedSources, fix: Optional[bool] = True, **kwargs) -> str: ...

    def get_query_context(
        self,
        template: AllowedTemplates,
        src: AllowedSources,
        **kwargs,
    ) -> dict: ...

    def create_table(self, src: AllowedSources, **kwargs) -> None: ...
