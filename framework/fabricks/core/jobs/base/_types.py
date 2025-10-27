from dataclasses import dataclass
from typing import List, Literal, Optional, TypedDict, Union

from pydantic import BaseModel, ConfigDict, model_validator
from pyspark.sql.types import StringType, StructField, StructType

from fabricks.cdc.base._types import AllowedChangeDataCaptures
from fabricks.context import BRONZE, GOLD, SILVER
from fabricks.core.jobs.get_job_id import get_dependency_id, get_job_id
from fabricks.core.parsers import ParserOptions
from fabricks.utils.fdict import FDict
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


class SparkOptions(TypedDict):
    sql: Optional[dict[str, str]]
    conf: Optional[dict[str, str]]


class ForeignKeyOptions(TypedDict):
    foreign_key: Optional[AllowedForeignKeyOptions]
    constraint: Optional[AllowedConstraintOptions]


class PrimaryKeyOptions(TypedDict):
    constraint: Optional[AllowedConstraintOptions]


class ForeignKey(TypedDict):
    keys: List[str]
    reference: str
    options: Optional[ForeignKeyOptions]


class PrimaryKey(TypedDict):
    keys: List[str]
    options: Optional[PrimaryKeyOptions]


class TableOptions(TypedDict):
    identity: Optional[bool]
    liquid_clustering: Optional[bool]
    partition_by: Optional[List[str]]
    zorder_by: Optional[List[str]]
    cluster_by: Optional[List[str]]
    powerbi: Optional[bool]
    maximum_compatibility: Optional[bool]
    bloomfilter_by: Optional[List[str]]
    constraints: Optional[dict[str, str]]
    properties: Optional[dict[str, str]]
    comment: Optional[str]
    calculated_columns: Optional[dict[str, str]]
    masks: Optional[dict[str, str]]
    comments: Optional[dict[str, str]]
    retention_days: Optional[int]
    primary_key: Optional[dict[str, PrimaryKey]]
    foreign_keys: Optional[dict[str, ForeignKey]]


class _InvokeOptions(TypedDict):
    notebook: str
    timeout: int
    arguments: Optional[dict[str, str]]


class InvokerOptions(TypedDict):
    pre_run: Optional[List[_InvokeOptions]]
    run: Optional[List[_InvokeOptions]]
    post_run: Optional[List[_InvokeOptions]]


class ExtenderOptions(TypedDict):
    extender: str
    arguments: Optional[dict[str, str]]


class CheckOptions(TypedDict):
    skip: Optional[bool]
    pre_run: Optional[bool]
    post_run: Optional[bool]
    min_rows: Optional[int]
    max_rows: Optional[int]
    count_must_equal: Optional[str]


class BronzeOptions(TypedDict):
    type: Optional[AllowedTypes]
    mode: AllowedModesBronze
    uri: str
    parser: str
    source: str
    keys: Optional[List[str]]
    # default
    parents: Optional[List[str]]
    filter_where: Optional[str]
    optimize: Optional[bool]
    compute_statistics: Optional[bool]
    vacuum: Optional[bool]
    no_drop: Optional[bool]
    # extra
    encrypted_columns: Optional[List[str]]
    calculated_columns: Optional[dict[str, str]]
    operation: Optional[AllowedOperations]
    timeout: Optional[int]


class SilverOptions(TypedDict):
    type: Optional[AllowedTypes]
    mode: AllowedModesSilver
    change_data_capture: AllowedChangeDataCaptures
    # default
    parents: Optional[List[str]]
    filter_where: Optional[str]
    optimize: Optional[bool]
    compute_statistics: Optional[bool]
    vacuum: Optional[bool]
    no_drop: Optional[bool]
    # extra
    deduplicate: Optional[bool]
    stream: Optional[bool]
    # else
    order_duplicate_by: Optional[dict[str, str]]
    timeout: Optional[int]


class GoldOptions(TypedDict):
    type: Optional[AllowedTypes]
    mode: AllowedModesGold
    change_data_capture: AllowedChangeDataCaptures
    update_where: Optional[str]
    # default
    parents: Optional[List[str]]
    optimize: Optional[bool]
    compute_statistics: Optional[bool]
    vacuum: Optional[bool]
    no_drop: Optional[bool]
    # extra
    deduplicate: Optional[bool]  # remove duplicates on the keys and on the hash
    rectify_as_upserts: Optional[bool]  # convert reloads into upserts and deletes
    correct_valid_from: Optional[bool]  # update valid_from to '1900-01-01' for the first timestamp
    persist_last_timestamp: Optional[bool]  # persist the last timestamp to be used as a watermark for the next run
    # delete_missing: Optional[bool]  # delete missing records on update (to be implemented)
    # else
    table: Optional[str]
    notebook: Optional[bool]
    requirements: Optional[bool]
    timeout: Optional[int]
    metadata: Optional[bool]


StepOptions = Union[BronzeOptions, SilverOptions, GoldOptions]


@dataclass
class BaseJobConf:
    job_id: str
    topic: str
    item: str


@dataclass
class JobConfBronze(BaseJobConf):
    step: TBronze
    options: BronzeOptions
    table_options: Optional[TableOptions] = None
    parser_options: Optional[ParserOptions] = None
    check_options: Optional[CheckOptions] = None
    spark_options: Optional[SparkOptions] = None
    invoker_options: Optional[InvokerOptions] = None
    extender_options: Optional[List[ExtenderOptions]] = None
    tags: Optional[List[str]] = None
    comment: Optional[str] = None


@dataclass
class JobConfSilver(BaseJobConf):
    step: TSilver
    options: SilverOptions
    table_options: Optional[TableOptions] = None
    check_options: Optional[CheckOptions] = None
    spark_options: Optional[SparkOptions] = None
    invoker_options: Optional[InvokerOptions] = None
    extender_options: Optional[List[ExtenderOptions]] = None
    tags: Optional[List[str]] = None
    comment: Optional[str] = None


@dataclass
class JobConfGold(BaseJobConf):
    step: TGold
    options: Optional[GoldOptions]
    table_options: Optional[TableOptions] = None
    check_options: Optional[CheckOptions] = None
    spark_options: Optional[SparkOptions] = None
    invoker_options: Optional[InvokerOptions] = None
    extender_options: Optional[List[ExtenderOptions]] = None
    tags: Optional[List[str]] = None
    comment: Optional[str] = None


JobConf = Union[JobConfBronze, JobConfSilver, JobConfGold]


@dataclass
class Paths:
    storage: Path
    tmp: Path
    checkpoints: Path
    commits: Path
    schema: Path
    runtime: Path


@dataclass
class Options:
    job: FDict
    check: FDict
    table: FDict
    spark: FDict
    invokers: FDict
    extenders: List


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
