from typing import Generator, List

from fabricks.models.config.fabricks import FabricksConfig
from fabricks.models.config.jobs.bronze import BronzeJobConfig
from fabricks.models.config.runtime import RuntimeConfig
from fabricks.models.config.steps.bronze import BronzeStepConfig
from fabricks.models.resolvers.jobs.base import BaseJobConfigResolver


class BronzeJobConfigResolver(BaseJobConfigResolver):
    step: BronzeStepConfig
    job: BronzeJobConfig

    @classmethod
    def from_file(cls, step: str, topic: str, item: str) -> "BronzeJobConfigResolver":
        _fabricks = FabricksConfig.load()

        runtime_path = _fabricks.resolve_config_path()
        runtime_generator: Generator[RuntimeConfig, None, None] = RuntimeConfig.from_file(runtime_path, root="conf")
        runtime = next(runtime_generator)

        if runtime.bronze is None:
            raise ValueError("bronze not found in runtime configuration")

        steps: List[BronzeStepConfig] = runtime.bronze
        _step: BronzeStepConfig = list(filter(lambda s: s.name == step, steps))[0]

        jobs_path = _fabricks.resolve_runtime_path() / _step.path_options.runtime
        jobs_generator: Generator[BronzeJobConfig, None, None] = BronzeJobConfig.from_files(
            jobs_path, root="job", preferred_file_name=topic
        )
        jobs: List[BronzeJobConfig] = list(jobs_generator)
        job: BronzeJobConfig = list(filter(lambda j: j.topic == topic and j.item == item, jobs))[0]

        return cls(fabricks=_fabricks, runtime=runtime, step=_step, job=job)

    @classmethod
    def from_parts(
        cls,
        fabricks: FabricksConfig,
        runtime: RuntimeConfig,
        step: BronzeStepConfig,
        job: BronzeJobConfig,
    ) -> "BronzeJobConfigResolver":
        return cls(fabricks=fabricks, runtime=runtime, step=step, job=job)
