from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pyspark.sql import DataFrame

from fabricks.cdc.mixins._types import AllowedSources
from fabricks.metastore.table import Table


class CDCAbstract(ABC):
    @abstractmethod
    def get_query(self, src: AllowedSources, **kwargs) -> str: ...

    @abstractmethod
    def get_data(self, src: AllowedSources, **kwargs) -> DataFrame: ...

    @abstractmethod
    def create_table(
        self,
        src: AllowedSources,
        partitioning: Optional[bool] = False,
        partition_by: Optional[Union[List[str], str]] = None,
        identity: Optional[bool] = False,
        liquid_clustering: Optional[bool] = False,
        cluster_by: Optional[Union[List[str], str]] = None,
        properties: Optional[dict[str, str | bool | int]] = None,
        masks: Optional[dict[str, str]] = None,
        primary_key: Optional[dict[str, Any]] = None,
        foreign_keys: Optional[dict[str, Any]] = None,
        generated_columns: Optional[dict[str, str]] = None,
        comments: Optional[dict[str, Any]] = None,
        **kwargs,
    ): ...

    @abstractmethod
    def drop(self): ...

    @abstractmethod
    def create_or_replace_view(self, src: Union[Table, str], **kwargs): ...

    @abstractmethod
    def optimize_table(self): ...

    @abstractmethod
    def update_schema(self, src: AllowedSources, **kwargs): ...

    @abstractmethod
    def get_differences_with_deltatable(self, src: AllowedSources, **kwargs): ...

    @abstractmethod
    def overwrite_schema(self, src: AllowedSources): ...
