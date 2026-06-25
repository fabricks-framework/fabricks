import re
from typing import Optional, Union

from pyspark.sql.types import Row

from fabricks.core.jobs.config import JobConfig
from fabricks.core.jobs.job_abc import JobABC
from fabricks.core.jobs.mixins.checker import CheckerMixin
from fabricks.core.jobs.mixins.configurator import ConfiguratorMixin
from fabricks.core.jobs.mixins.generator import GeneratorMixin
from fabricks.core.jobs.mixins.invoker import InvokerMixin
from fabricks.core.jobs.mixins.processor import ProcessorMixin
from fabricks.core.udfs import UDF_PREFIX

_UDF_PATTERN = re.compile(rf"(?<={UDF_PREFIX})\w*(?=\()")


class BaseJob(ProcessorMixin, InvokerMixin, CheckerMixin, GeneratorMixin, ConfiguratorMixin, JobABC):
    def __init__(
        self,
        expand: str,
        step: str,
        topic: Optional[str] = None,
        item: Optional[str] = None,
        job_id: Optional[str] = None,
        conf: Optional[Union[dict, Row]] = None,
    ):
        self.config = JobConfig(expand, step, topic=topic, item=item, job_id=job_id, row=conf)
        self.expand = self.config.expand
        self.step = self.config.step
        self.topic = self.config.topic
        self.item = self.config.item
        self.conf = self.config.conf
        self.job_id = self.config.job_id
