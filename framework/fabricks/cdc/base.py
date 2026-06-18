from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc.config import AllowedSources, CDCConfig
from fabricks.cdc.delegates import CDCDba, CDCMerger, CDCQuery
from fabricks.metastore.database import Database
from fabricks.metastore.table import SchemaDiff, Table
from fabricks.models.cdc import CDCMergeContext, CDCQueryContext
from fabricks.utils._types import DataFrameLike


class BaseCDC:
    def __init__(
        self,
        database: str,
        *levels: str,
        change_data_capture: str,
        spark: Optional[SparkSession] = None,
    ):
        self._config = CDCConfig(database, *levels, change_data_capture=change_data_capture, spark=spark)
        self._dba = CDCDba(self)
        self._query = CDCQuery(self)
        self._merger = CDCMerger(self)

    # --- substrate (delegated to CDCConfig) ---

    @property
    def spark(self) -> SparkSession:
        return self._config.spark

    @property
    def database(self) -> Database:
        return self._config.database

    @property
    def levels(self):
        return self._config.levels

    @property
    def change_data_capture(self) -> str:
        return self._config.change_data_capture

    @property
    def table(self) -> Table:
        return self._config.table

    @property
    def is_view(self) -> bool:
        return self._config.is_view

    @property
    def registered(self) -> bool:
        return self._config.registered

    @property
    def qualified_name(self) -> str:
        return self._config.qualified_name

    @property
    def slowly_changing_dimension(self) -> bool:
        return self._config.slowly_changing_dimension

    @property
    def allowed_input__columns(self) -> List[str]:
        return self._config.allowed_input__columns

    @property
    def allowed_ouput_leading__columns(self) -> List[str]:
        return self._config.allowed_ouput_leading__columns

    @property
    def allowed_output_trailing__columns(self) -> List[str]:
        return self._config.allowed_output_trailing__columns

    def get_src(self, src: AllowedSources) -> DataFrameLike:
        return self._config.get_src(src)

    def has_data(self, src: AllowedSources, **kwargs) -> bool:
        return self._config.has_data(src, **kwargs)

    def get_columns(
        self,
        src: AllowedSources,
        backtick: Optional[bool] = True,
        sort: Optional[bool] = True,
        check: Optional[bool] = True,
    ) -> List[str]:
        return self._config.get_columns(src, backtick=backtick, sort=sort, check=check)

    def sort_columns(self, columns: List[str]) -> List[str]:
        return self._config.sort_columns(columns)

    def reorder_dataframe(self, df: DataFrame, extra__columns: Optional[List[str]] = None) -> DataFrame:
        return self._config.reorder_dataframe(df, extra__columns=extra__columns)

    # --- DDL & schema (delegated to CDCDba) ---

    def drop(self):
        self._dba.drop()

    def create_table(self, src: AllowedSources, **kwargs):
        self._dba.create_table(src, **kwargs)

    def create_or_replace_view(self, src: Union[Any, str], schema_evolution: bool = True, **kwargs):
        self._dba.create_or_replace_view(src, schema_evolution=schema_evolution, **kwargs)

    def optimize_table(self):
        self._dba.optimize_table()

    def get_differences_with_deltatable(self, src: AllowedSources, **kwargs):
        return self._dba.get_differences_with_deltatable(src, **kwargs)

    def get_schema_differences(self, src: AllowedSources, **kwargs) -> Optional[Sequence[SchemaDiff]]:
        return self._dba.get_schema_differences(src, **kwargs)

    def schema_drifted(self, src: AllowedSources, **kwargs) -> Optional[bool]:
        return self._dba.schema_drifted(src, **kwargs)

    def update_schema(self, src: AllowedSources, **kwargs):
        self._dba.update_schema(src, **kwargs)

    def overwrite_schema(self, src: AllowedSources, **kwargs):
        self._dba.overwrite_schema(src, **kwargs)

    # --- query pipeline (delegated to CDCQuery) ---

    def get_data(self, src: AllowedSources, **kwargs) -> DataFrame:
        return self._query.get_data(src, **kwargs)

    def get_query_context(self, template, src: AllowedSources, **kwargs) -> CDCQueryContext:
        return self._query.get_query_context(template, src, **kwargs)

    def fix_sql(self, sql: str) -> str:
        return self._query.fix_sql(sql)

    def fix_context(self, context: CDCQueryContext, fix: Optional[bool] = True, **kwargs) -> CDCQueryContext:
        return self._query.fix_context(context, fix=fix, **kwargs)

    def get_query(self, src: AllowedSources, fix: Optional[bool] = True, **kwargs) -> str:
        return self._query.get_query(src, fix=fix, **kwargs)

    def append(self, src: AllowedSources, **kwargs):
        self._query.append(src, **kwargs)

    def overwrite(self, src: AllowedSources, dynamic: Optional[bool] = False, **kwargs):
        self._query.overwrite(src, dynamic=dynamic, **kwargs)

    # --- merge (delegated to CDCMerger) ---

    def get_merge_context(self, src: Union[DataFrame, str], **kwargs) -> CDCMergeContext:
        return self._merger.get_merge_context(src, **kwargs)

    def get_merge_query(self, src: Union[DataFrame, str], fix: Optional[bool] = True, **kwargs) -> str:
        return self._merger.get_merge_query(src, fix=fix, **kwargs)

    def merge(self, src: AllowedSources, **kwargs):
        self._merger.merge(src, **kwargs)

    def __str__(self) -> str:
        return str(self._config)
