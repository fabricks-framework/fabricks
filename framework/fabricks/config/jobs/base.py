from typing import List, Literal, Optional

from fabricks.config.base import ModelBase

FileFormats = Literal["json_array", "json", "jsonl", "csv", "parquet", "delta"]
Operations = Literal["upsert", "reload", "delete"]
Types = Literal["manual", "default"]
Origins = Literal["parser", "job"]
ChangeDataCaptures = Literal["none", "nocdc", "scd1", "scd2"]
Modes = Literal[
    "memory",
    "append",
    "complete",
    "update",
    "invoke",
    "memory",
    "append",
    "register",
    "memory",
    "append",
    "latest",
    "update",
    "combine",
]
Steps = Literal["bronze", "silver", "gold"]


class SparkOptions(ModelBase):
    sql: Optional[dict[str, str]] = None
    conf: Optional[dict[str, str]] = None


class TableOptions(ModelBase):
    identity: Optional[bool] = None
    liquid_clustering: Optional[bool] = None
    partition_by: Optional[List[str]] = None
    zorder_by: Optional[List[str]] = None
    cluster_by: Optional[List[str]] = None
    powerbi: Optional[bool] = None
    bloomfilter_by: Optional[List[str]] = None
    constraints: Optional[dict[str, str]] = None
    properties: Optional[dict[str, str]] = None
    comment: Optional[str] = None
    calculated_columns: Optional[dict[str, str]] = None
    retention_days: Optional[int] = None


class InvokeOptions(ModelBase):
    notebook: str
    timeout: int
    arguments: Optional[dict[str, str]] = None


class InvokerOptions(ModelBase):
    pre_run: Optional[List[InvokeOptions]] = None
    run: Optional[List[InvokeOptions]] = None
    post_run: Optional[List[InvokeOptions]] = None


class ExtenderOptions(ModelBase):
    extender: str
    arguments: Optional[dict[str, str]] = None


class CheckOptions(ModelBase):
    skip: Optional[bool] = None
    pre_run: Optional[bool] = None
    post_run: Optional[bool] = None
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    count_must_equal: Optional[str] = None


class DefaultOptions(ModelBase):
    type: Optional[Types] = None
    mode: Modes
    change_data_capture: Optional[ChangeDataCaptures]
    # extra
    parents: Optional[List[str]] = None
    filter_where: Optional[str] = None
    timeout: Optional[int] = None


class BaseJobConfig(ModelBase):
    job_id: str

    extend: Steps
    step: Steps

    topic: str
    item: str

    options: Optional[DefaultOptions] = None
    table_options: Optional[TableOptions] = None
    check_options: Optional[CheckOptions] = None
    spark_options: Optional[SparkOptions] = None
    invoker_options: Optional[InvokerOptions] = None
    extender_options: Optional[List[ExtenderOptions]] = None

    tags: Optional[List[str]] = None
    comment: Optional[str] = None
