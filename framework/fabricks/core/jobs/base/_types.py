from dataclasses import dataclass
from typing import List, Literal, Optional, TypedDict, Union

from pyspark.sql.types import StringType, StructField, StructType

from fabricks.cdc.base._types import ChangeDataCaptures
from fabricks.context import BRONZE, GOLD, SILVER
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

BronzeModes = Literal["memory", "append", "register"]
SilverModes = Literal["memory", "append", "latest", "update", "combine"]
GoldModes = Literal["memory", "append", "complete", "update", "invoke"]
Modes = Literal[BronzeModes, SilverModes, GoldModes]

FileFormats = Literal["json_array", "json", "jsonl", "csv", "parquet", "delta"]
Operations = Literal["upsert", "reload", "delete"]
Types = Literal["manual", "default"]


class SparkOptions(TypedDict):
    sql: Optional[dict[str, str]]
    conf: Optional[dict[str, str]]


class TableOptions(TypedDict):
    identity: Optional[bool]
    liquid_clustering: Optional[bool]
    partition_by: Optional[List[str]]
    zorder_by: Optional[List[str]]
    cluster_by: Optional[List[str]]
    powerbi: Optional[bool]
    bloomfilter_by: Optional[List[str]]
    constraints: Optional[dict[str, str]]
    properties: Optional[dict[str, str]]
    comment: Optional[str]
    calculated_columns: Optional[dict[str, str]]
    retention_days: Optional[int]


class _InvokeOptions(TypedDict):
    notebook: str
    timeout: int
    arguments: Optional[dict[str, str]]


class InvokerOptions(TypedDict):
    pre_run: Optional[List[_InvokeOptions]]
    run: Optional[_InvokeOptions]
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
    type: Optional[Types]
    mode: BronzeModes
    uri: str
    parser: str
    source: str
    keys: Optional[List[str]]
    # default
    parents: Optional[List[str]]
    filter_where: Optional[str]
    # extra
    encrypted_columns: Optional[List[str]]
    calculated_columns: Optional[dict[str, str]]
    operation: Optional[Operations]
    timeout: Optional[int]


class SilverOptions(TypedDict):
    type: Optional[Types]
    mode: SilverModes
    change_data_capture: ChangeDataCaptures
    # default
    parents: Optional[List[str]]
    filter_where: Optional[str]
    # extra
    deduplicate: Optional[bool]
    stream: Optional[bool]
    # else
    order_duplicate_by: Optional[dict[str, str]]
    timeout: Optional[int]


class GoldOptions(TypedDict):
    type: Optional[Types]
    mode: GoldModes
    change_data_capture: ChangeDataCaptures
    update_where: Optional[str]
    # default
    parents: Optional[List[str]]
    # extra
    deduplicate: Optional[bool]
    correct_valid_from: Optional[bool]
    persist_last_timestamp: Optional[bool]
    # else
    table: Optional[str]
    notebook: Optional[bool]
    requirements: Optional[bool]
    timeout: Optional[int]


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


SchemaDependencies = StructType(
    [
        StructField("dependency_id", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("parent_id", StringType(), True),
        StructField("parent", StringType(), True),
    ]
)
