from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Tuple, Union

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc.mixins._types import AllowedSources, AllowedTemplates
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table
from fabricks.models.cdc import CdcContext, QueryContext


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

    def get_data(self, src: AllowedSources, context: CdcContext) -> DataFrame: ...

    def get_query(self, src: AllowedSources, context: CdcContext, fix: Optional[bool] = True) -> str: ...

    def get_query_context(self, template: AllowedTemplates, src: AllowedSources, **kwargs) -> QueryContext: ...

    def create_table(
        self,
        src: AllowedSources,
        context: CdcContext,
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[Dict[str, Any]] = None,
        masks: Optional[Dict[str, str]] = None,
        primary_key: Optional[Dict[str, Any]] = None,
        foreign_keys: Optional[Dict[str, Any]] = None,
        generated_columns: Optional[Dict[str, str]] = None,
        comments: Optional[Dict[str, Any]] = None,
    ) -> None: ...

    def update_schema(self, src: AllowedSources, context: CdcContext, widen_types: bool = False) -> None: ...
