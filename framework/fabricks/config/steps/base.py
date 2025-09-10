from __future__ import annotations

from typing import Any, Dict, List, Optional

from fabricks.config.base import ModelBase


class PathOptions(ModelBase):
    runtime: str
    storage: str


class StepTimeoutOptions(ModelBase):
    step: Optional[int] = None
    job: Optional[int] = None
    pre_run: Optional[int] = None
    post_run: Optional[int] = None


class InvokeOptions(ModelBase):
    notebook: str
    arguments: Optional[Dict[str, Any]] = None


class ExtenderOptions(ModelBase):
    extender: str
    arguments: Optional[Dict[str, Any]] = None


class TableOptions(ModelBase):
    powerbi: Optional[bool] = None
    liquid_clustering: Optional[bool] = None
    properties: Optional[Dict[str, Any]] = None
    retention_days: Optional[int] = None


class DefaultOptions(ModelBase):
    order: int
    workers: Optional[int] = None
    timeouts: StepTimeoutOptions
    extenders: Optional[List[str]] = None
    pre_run: Optional[InvokeOptions] = None
    post_run: Optional[InvokeOptions] = None


class BaseStepConfig(ModelBase):
    name: str
    options: DefaultOptions
    path_options: PathOptions
    table_options: Optional[TableOptions] = None
