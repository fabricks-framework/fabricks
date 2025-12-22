from typing import List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, computed_field, model_validator
from pyspark.sql.types import StringType, StructField, StructType

from fabricks.cdc.base._types import AllowedChangeDataCaptures
from fabricks.context import BRONZE, GOLD, SILVER
from fabricks.core.jobs.get_job_id import get_dependency_id, get_job_id
from fabricks.core.parsers import ParserOptions
from fabricks.utils.path import Path

TBronze = Literal["bronze"]
TSilver = Literal["silver"]
TGold = Literal["gold"]
TStep = Literal[TBronze, TSilver, TGold]

Bronzes: List[TBronze] = [b.get("name") for b in BRONZE]
Silvers: List[TSilver] = [s.get("name") for s in SILVER]
Golds: List[TGold] = [g.get("name") for g in GOLD]
Steps: List[TStep] = Bronzes + Silvers + Golds

AllowedModesBronze = Literal["memory", "append", "register"]
AllowedModesSilver = Literal["memory", "append", "latest", "update", "combine"]
AllowedModesGold = Literal["memory", "append", "complete", "update", "invoke"]
AllowedModes = Literal[AllowedModesBronze, AllowedModesSilver, AllowedModesGold]

AllowedFileFormats = Literal["json_array", "json", "jsonl", "csv", "parquet", "delta"]
AllowedOperations = Literal["upsert", "reload", "delete"]
AllowedTypes = Literal["manual", "default"]
AllowedOrigins = Literal["parser", "job"]

AllowedConstraintOptions = Literal["not enforced", "deferrable", "initially deferred", "norely", "rely"]
AllowedForeignKeyOptions = Literal["match full", "on update no action", "on delete no action"]


class SparkOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sql: Optional[dict[str, str]] = None
    conf: Optional[dict[str, str]] = None


class ForeignKeyOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    foreign_key: Optional[AllowedForeignKeyOptions] = None
    constraint: Optional[AllowedConstraintOptions] = None


class PrimaryKeyOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    constraint: Optional[AllowedConstraintOptions] = None


class ForeignKey(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    keys: List[str]
    reference: str
    options: Optional[ForeignKeyOptions] = None


class PrimaryKey(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    keys: List[str]
    options: Optional[PrimaryKeyOptions] = None


class TableOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity: Optional[bool] = None
    liquid_clustering: Optional[bool] = None
    partition_by: Optional[List[str]] = None
    zorder_by: Optional[List[str]] = None
    cluster_by: Optional[List[str]] = None
    powerbi: Optional[bool] = None
    maximum_compatibility: Optional[bool] = None
    bloomfilter_by: Optional[List[str]] = None
    constraints: Optional[dict[str, str]] = None
    properties: Optional[dict[str, str]] = None
    comment: Optional[str] = None
    calculated_columns: Optional[dict[str, str]] = None
    masks: Optional[dict[str, str]] = None
    comments: Optional[dict[str, str]] = None
    retention_days: Optional[int] = None
    primary_key: Optional[dict[str, PrimaryKey]] = None
    foreign_keys: Optional[dict[str, ForeignKey]] = None


class _InvokeOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    notebook: str
    timeout: int
    arguments: Optional[dict[str, str]] = None


class InvokerOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    pre_run: Optional[List[_InvokeOptions]] = None
    run: Optional[List[_InvokeOptions]] = None
    post_run: Optional[List[_InvokeOptions]] = None


class ExtenderOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    extender: str
    arguments: Optional[dict[str, str]] = None


class CheckOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    skip: Optional[bool] = None
    pre_run: Optional[bool] = None
    post_run: Optional[bool] = None
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    count_must_equal: Optional[str] = None


class BronzeOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesBronze
    change_data_capture: AllowedChangeDataCaptures = "none"

    uri: str
    parser: str
    source: str
    type: Optional[AllowedTypes] = None
    keys: Optional[List[str]] = None
    # default
    parents: Optional[List[str]] = None
    filter_where: Optional[str] = None
    optimize: Optional[bool] = None
    compute_statistics: Optional[bool] = None
    vacuum: Optional[bool] = None
    no_drop: Optional[bool] = None
    # extra
    encrypted_columns: Optional[List[str]] = None
    calculated_columns: Optional[dict[str, str]] = None
    operation: Optional[AllowedOperations] = None
    timeout: Optional[int] = None


class SilverOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesSilver
    change_data_capture: AllowedChangeDataCaptures
    type: Optional[AllowedTypes] = None
    # default
    parents: Optional[List[str]] = None
    filter_where: Optional[str] = None
    optimize: Optional[bool] = None
    compute_statistics: Optional[bool] = None
    vacuum: Optional[bool] = None
    no_drop: Optional[bool] = None
    # extra
    deduplicate: Optional[bool] = None
    stream: Optional[bool] = None
    # else
    order_duplicate_by: Optional[dict[str, str]] = None
    timeout: Optional[int] = None


class GoldOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesGold
    change_data_capture: AllowedChangeDataCaptures
    type: Optional[AllowedTypes] = None
    update_where: Optional[str] = None
    # default
    parents: Optional[List[str]] = None
    optimize: Optional[bool] = None
    compute_statistics: Optional[bool] = None
    vacuum: Optional[bool] = None
    no_drop: Optional[bool] = None
    # extra
    deduplicate: Optional[bool] = None  # remove duplicates on the keys and on the hash
    rectify_as_upserts: Optional[bool] = None  # convert reloads into upserts and deletes
    correct_valid_from: Optional[bool] = None  # update valid_from to '1900-01-01' for the first timestamp
    # persist timestamp to be used as a watermark for the next run
    persist_last_timestamp: Optional[bool] = None
    persist_last_updated_timestamp: Optional[bool] = None
    # delete_missing: Optional[bool] = None  # delete missing records on update (to be implemented)
    table: Optional[str] = None
    notebook: Optional[bool] = None
    requirements: Optional[bool] = None
    timeout: Optional[int] = None
    # tracking
    metadata: Optional[bool] = None
    last_updated: Optional[bool] = None


TOptions = Union[BronzeOptions, SilverOptions, GoldOptions]


class JobConfBase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    step: TStep
    topic: str
    item: str

    @computed_field  # type: ignore[misc]
    @property
    def job_id(self) -> str:
        """Computed job_id from step, topic, and item."""
        return get_job_id(step=self.step, topic=self.topic, item=self.item)

    options: TOptions
    table_options: Optional[TableOptions] = None
    check_options: Optional[CheckOptions] = None
    spark_options: Optional[SparkOptions] = None
    invoker_options: Optional[InvokerOptions] = None
    extender_options: Optional[List[ExtenderOptions]] = None
    tags: Optional[List[str]] = None
    comment: Optional[str] = None


class JobConfBronze(JobConfBase):
    step: TBronze

    options: BronzeOptions
    parser_options: Optional[ParserOptions] = None


class JobConfSilver(JobConfBase):
    step: TSilver

    options: SilverOptions


class JobConfGold(JobConfBase):
    step: TGold

    options: GoldOptions


JobConf = Union[JobConfBronze, JobConfSilver, JobConfGold]


class Paths(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    storage: Path
    tmp: Path
    checkpoints: Path
    commits: Path
    schema: Path
    runtime: Path


class JobDependency(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    origin: AllowedOrigins
    job_id: str
    parent: str
    parent_id: str
    dependency_id: str

    def __str__(self) -> str:
        return f"{self.job_id} -> {self.parent}"

    @model_validator(mode="after")
    def check_no_circular_dependency(self):
        if self.job_id == self.parent_id:
            raise ValueError("Circular dependency detected")
        return self

    @staticmethod
    def from_parts(job_id: str, parent: str, origin: AllowedOrigins):
        parent = parent.removesuffix("__current")
        return JobDependency(
            job_id=job_id,
            origin=origin,
            parent=parent,
            parent_id=get_job_id(job=parent),
            dependency_id=get_dependency_id(parent=parent, job_id=job_id),
        )


SchemaDependencies = StructType(
    [
        StructField("dependency_id", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("parent_id", StringType(), True),
        StructField("parent", StringType(), True),
    ]
)
