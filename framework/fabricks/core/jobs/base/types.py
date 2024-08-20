from dataclasses import dataclass
from typing import List, Literal, Optional, TypedDict, Union

from fabricks.cdc.base.types import ChangeDataCaptures
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
    arguments: Optional[dict[str, str]]


class InvokerOptions(TypedDict):
    notebook: str
    arguments: Optional[dict[str, str]]
    pre_run: Optional[_InvokeOptions]
    post_run: Optional[_InvokeOptions]


class CheckOptions(TypedDict):
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
    extender: Optional[str]
    # extra
    encrypted_columns: Optional[List[str]]
    calculated_columns: Optional[dict[str, str]]
    operation: Optional[Operations]


class SilverOptions(TypedDict):
    type: Optional[Types]
    mode: SilverModes
    change_data_capture: ChangeDataCaptures
    # default
    parents: Optional[List[str]]
    filter_where: Optional[str]
    extender: Optional[str]
    # extra
    deduplicate: Optional[bool]
    stream: Optional[bool]
    # else
    order_duplicate_by: Optional[dict[str, str]]


class GoldOptions(TypedDict):
    type: Optional[Types]
    mode: GoldModes
    change_data_capture: ChangeDataCaptures
    update_where: Optional[str]
    # default
    parents: Optional[List[str]]
    # extra
    deduplicate: Optional[bool]
    # else
    table: Optional[str]
    notebook: Optional[bool]
    requirements: Optional[bool]


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
    invoker: FDict


@dataclass
class Timeouts:
    job: int
    pre_run: int
    post_run: int
