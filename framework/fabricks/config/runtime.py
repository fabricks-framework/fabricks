from __future__ import annotations

from typing import Any, Dict, List, Optional

from fabricks.config.steps.base import ExtenderOptions, ModelBase
from fabricks.config.steps.bronze import BronzeStepConfig
from fabricks.config.steps.gold import GoldStepConfig
from fabricks.config.steps.silver import SilverStepConfig


class RuntimePathOptions(ModelBase):
    storage: str
    udfs: str
    parsers: str
    schedules: str
    views: str
    requirements: str


class RuntimeTimeoutOptions(ModelBase):
    step: int
    job: int
    pre_run: int
    post_run: int


class RuntimeOptions(ModelBase):
    secret_scope: str
    unity_catalog: Optional[bool] = None
    type_widening: Optional[bool] = None
    catalog: Optional[str] = None
    workers: int
    timeouts: RuntimeTimeoutOptions
    retention_days: int


class SparkOptions(ModelBase):
    sql: Dict[str, Any]
    conf: Dict[str, Any]


class PowerBI(ModelBase):
    name: str


class DatabasePathOptions(ModelBase):
    storage: str


class Database(ModelBase):
    name: str
    path_options: DatabasePathOptions


class RuntimeConfig(ModelBase):
    name: str
    options: RuntimeOptions
    path_options: RuntimePathOptions
    extender_options: Optional[ExtenderOptions] = None
    spark_options: SparkOptions
    bronze: Optional[List[BronzeStepConfig]] = None
    silver: Optional[List[SilverStepConfig]] = None
    gold: Optional[List[GoldStepConfig]] = None
    powerbi: Optional[List[PowerBI]] = None
    databases: Optional[List[Database]] = None
    variables: Optional[List[Dict[str, Any]]] = None
    credentials: Optional[List[Dict[str, Any]]] = None
