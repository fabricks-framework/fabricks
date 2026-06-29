from typing import Iterable, List, Literal, Optional, Protocol, Union, runtime_checkable

from pyspark.sql import DataFrame

from fabricks.core.steps._types import Modes, Timeouts
from fabricks.metastore.database import Database
from fabricks.models.step import StepOptions


@runtime_checkable
class StepProtocol(Protocol):
    name: str
    expand: str
    database: Database

    @property
    def workers(self) -> int: ...
    @property
    def timeouts(self) -> Timeouts: ...
    @property
    def conf(self) -> dict: ...
    @property
    def options(self) -> StepOptions: ...
    def drop(self) -> None: ...
    def create(
        self,
        mode: Optional[Modes] = ...,
        max_retries: Optional[int] = ...,
    ) -> None: ...
    def update(
        self,
        mode: Optional[Modes] = ...,
        update_dependencies: Optional[bool] = ...,
        progress_bar: Optional[bool] = ...,
        incremental: Optional[bool] = ...,
        max_retries: Optional[int] = ...,
    ) -> None: ...
    def get_jobs_iter(self, topic: Optional[str] = ...) -> Iterable[dict]: ...
    def get_jobs(self, topic: Optional[str] = ...) -> DataFrame: ...
    def get_dependencies(
        self,
        progress_bar: Optional[bool] = ...,
        topic: Optional[Union[str, List[str]]] = ...,
        include_manual: Optional[bool] = ...,
        loglevel: Optional[Literal[10, 20, 30, 40, 50]] = ...,
    ) -> DataFrame: ...
    def create_db_objects(
        self,
        mode: Optional[Modes] = ...,
        max_retries: Optional[int] = ...,
        update_lists: Optional[bool] = ...,
        incremental: Optional[bool] = ...,
    ) -> None: ...
    def update_dependencies(
        self,
        progress_bar: Optional[bool] = ...,
        topic: Optional[Union[str, List[str]]] = ...,
        include_manual: Optional[bool] = ...,
        loglevel: Optional[Literal[10, 20, 30, 40, 50]] = ...,
    ) -> None: ...
    def register(self, update: Optional[bool] = ..., drop: Optional[bool] = ...) -> None: ...
    def update_steps_list(self) -> None: ...
    def update_views_list(self) -> None: ...
    def update_tables_list(self) -> None: ...
    def update_configurations(self, drop: Optional[bool] = ...) -> None: ...
    def __str__(self) -> str: ...
