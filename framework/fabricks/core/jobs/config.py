from functools import cached_property
from typing import List, Optional, Union

from pyspark.sql.types import Row

from fabricks.context import PATHS_RUNTIME, PATHS_STORAGE, STEPS
from fabricks.core.jobs.get_job_conf import get_job_conf
from fabricks.models import (
    CheckOptions,
    ExtenderOptions,
    InvokerOptions,
    Paths,
    RuntimeConf,
    RuntimeOptions,
    SparkOptions,
    StepBronzeConf,
    StepGoldConf,
    StepSilverConf,
    StepTableOptions,
    TableOptions,
    UpdaterOptions,
    get_job_id,
)


class JobConfig:
    def __init__(
        self,
        expand: str,
        step: str,
        topic: Optional[str] = None,
        item: Optional[str] = None,
        job_id: Optional[str] = None,
        row: Optional[Union[dict, Row]] = None,
    ):
        self.expand = expand
        self.step = step

        if job_id is not None:
            self.job_id = job_id
            self.conf = get_job_conf(step=step, job_id=job_id, row=row)
            self.topic = self.conf.topic
            self.item = self.conf.item
        else:
            assert topic
            assert item
            self.topic = topic
            self.item = item
            self.conf = get_job_conf(step=step, topic=topic, item=item, row=row)
            self.job_id = get_job_id(step=step, topic=topic, item=item)

    @cached_property
    def base_step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]:
        return STEPS[self.step]

    @property
    def qualified_name(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"

    @cached_property
    def paths(self) -> Paths:
        storage = PATHS_STORAGE.get(self.step)
        assert storage
        runtime_root = PATHS_RUNTIME.get(self.step)
        assert runtime_root
        return Paths(
            to_storage=storage,
            to_tmp=storage.joinpath("tmp", self.topic, self.item),
            to_checkpoints=storage.joinpath("checkpoints", self.topic, self.item),
            to_commits=storage.joinpath("checkpoints", self.topic, self.item, "commits"),
            to_schema=storage.joinpath("schema", self.topic, self.item),
            to_runtime=runtime_root.joinpath(self.topic, self.item),
        )

    @cached_property
    def step_table_options(self) -> Optional[StepTableOptions]:
        return STEPS[self.step].table_options

    @cached_property
    def runtime_conf(self) -> RuntimeConf:
        from fabricks.context.runtime import CONF_RUNTIME

        return CONF_RUNTIME

    @property
    def runtime_options(self) -> RuntimeOptions:
        return self.runtime_conf.options

    @property
    def table_options(self) -> Optional[TableOptions]:
        return self.conf.table_options

    @property
    def check_options(self) -> Optional[CheckOptions]:
        return self.conf.check_options

    @property
    def spark_options(self) -> Optional[SparkOptions]:
        return self.conf.spark_options

    @property
    def invoker_options(self) -> Optional[InvokerOptions]:
        return self.conf.invoker_options

    @property
    def updater_options(self) -> Optional[UpdaterOptions]:
        return self.conf.updater_options

    @property
    def extender_options(self) -> Optional[List[ExtenderOptions]]:
        return self.conf.extender_options

    def __str__(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"
