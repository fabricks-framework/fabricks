from typing import List, Optional, TypedDict


class RuntimePathOptions(TypedDict):
    storage: str
    udfs: str
    parsers: str
    schedules: str
    views: str
    requirements: str


class RuntimeTimeoutOptions(TypedDict):
    step: int
    job: int
    pre_run: int
    post_run: int


class StepTimeoutOptions(TypedDict):
    step: Optional[int]
    job: Optional[int]
    pre_run: Optional[int]
    post_run: Optional[int]


class RuntimeOptions(TypedDict):
    secret_scope: str
    unity_catalog: bool
    catalog: str
    workers: int
    timeouts: RuntimeTimeoutOptions
    retention_days: int


class SparkOptions(TypedDict):
    sql: dict
    conf: dict


class StepPathOptions(TypedDict):
    runtime: str
    storage: str


class InvokeOptions(TypedDict):
    notebook: str
    arguments: Optional[dict[str, str]]


class ExtenderOptions(TypedDict):
    extender: str
    arguments: Optional[dict[str, str]]


class StepOptions(TypedDict):
    order: int
    workers: Optional[int]
    timeouts: StepTimeoutOptions
    extenders: Optional[List[str]]
    pre_run: Optional[InvokeOptions]
    post_run: Optional[InvokeOptions]


class SilverOptions(StepOptions):
    parent: str
    stream: Optional[bool]
    local_checkpoint: Optional[bool]


class GoldOptions(StepOptions):
    schema_drift: Optional[bool]


class Step(TypedDict):
    name: str


class TableOptions(TypedDict):
    powerbi: Optional[bool]
    liquid_clustering: Optional[bool]
    properties: Optional[dict[str, str]]
    retention_days: Optional[int]


class Bronze(Step):
    options: StepOptions
    path_options: StepPathOptions
    table_options: Optional[TableOptions]


class Silver(Step):
    options: SilverOptions
    path_options: StepPathOptions
    table_options: Optional[TableOptions]


class Gold(Step):
    options: GoldOptions
    path_options: StepPathOptions
    table_options: Optional[TableOptions]


class PowerBI(Step):
    pass


class DatabasePathOptions(TypedDict):
    storage: str


class Database(TypedDict):
    name: str
    path_options: DatabasePathOptions


class Conf(TypedDict):
    name: str
    options: RuntimeOptions
    path_options: RuntimePathOptions
    extender_options: Optional[ExtenderOptions]
    spark_options: SparkOptions
    bronze: Optional[List[Bronze]]
    silver: Optional[List[Silver]]
    gold: Optional[List[Gold]]
    powerbi: Optional[List[PowerBI]]
    databases: Optional[List[Database]]
    variables: Optional[List[dict[str, str]]]
    credentials: Optional[List[dict[str, str]]]
