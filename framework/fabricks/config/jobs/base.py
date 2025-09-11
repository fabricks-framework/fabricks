from typing import Any, List, Literal, Optional

from pydantic import computed_field

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
    sql: Optional[dict[str, Any]] = None
    conf: Optional[dict[str, Any]] = None


class TableOptions(ModelBase):
    identity: Optional[bool] = None
    liquid_clustering: Optional[bool] = None
    partition_by: Optional[List[str]] = None
    zorder_by: Optional[List[str]] = None
    cluster_by: Optional[List[str]] = None
    powerbi: Optional[bool] = None
    bloomfilter_by: Optional[List[str]] = None
    constraints: Optional[dict[str, Any]] = None
    properties: Optional[dict[str, Any]] = None
    comment: Optional[str] = None
    calculated_columns: Optional[dict[str, Any]] = None
    retention_days: Optional[int] = None


class InvokeOptions(ModelBase):
    notebook: str
    timeout: Optional[int] = None
    arguments: Optional[dict[str, Any]] = None


class InvokerOptions(ModelBase):
    pre_run: Optional[List[InvokeOptions]] = None
    run: Optional[List[InvokeOptions]] = None
    post_run: Optional[List[InvokeOptions]] = None


class ExtenderOptions(ModelBase):
    extender: str
    arguments: Optional[dict[str, Any]] = None


class CheckOptions(ModelBase):
    skip: Optional[bool] = None
    pre_run: Optional[bool] = None
    post_run: Optional[bool] = None
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    count_must_equal: Optional[str] = None


class DefaultOptions(ModelBase):
    mode: Modes
    change_data_capture: ChangeDataCaptures = "none"

    # preferred
    type: Optional[Types] = None

    # extra
    parents: Optional[List[str]] = None
    filter_where: Optional[str] = None
    timeout: Optional[int] = None


class BaseJobConfig(ModelBase):
    extend: Steps

    step: str
    topic: str
    item: str

    options: DefaultOptions
    table_options: Optional[TableOptions] = None
    check_options: Optional[CheckOptions] = None
    spark_options: Optional[SparkOptions] = None
    invoker_options: Optional[InvokerOptions] = None
    extender_options: Optional[List[ExtenderOptions]] = None

    tags: Optional[List[str]] = None
    comment: Optional[str] = None

    @computed_field
    @property
    def job_id(self) -> str:
        from hashlib import md5

        md5 = md5(self.job.encode())
        return md5.hexdigest()

    @computed_field
    def job(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"
