from typing import Generator, List

from fabricks.models.config.fabricks import FabricksConfig
from fabricks.models.config.jobs.silver import SilverJobConfig
from fabricks.models.config.runtime import RuntimeConfig
from fabricks.models.config.steps.silver import SilverStepConfig
from fabricks.models.resolvers.jobs.base import BaseJobConfigResolver


class SilverJobConfigResolver(BaseJobConfigResolver):
    step: SilverStepConfig
    job: SilverJobConfig

    @classmethod
    def from_file(cls, step: str, topic: str, item: str) -> "SilverJobConfigResolver":
        _fabricks = FabricksConfig.load()

        runtime_path = _fabricks.resolve_config_path()
        runtime_generator: Generator[RuntimeConfig, None, None] = RuntimeConfig.from_file(runtime_path, root="conf")
        runtime = next(runtime_generator)

        if runtime.silver is None:
            raise ValueError("silver not found in runtime configuration")

        steps: List[SilverStepConfig] = runtime.silver
        _step: SilverStepConfig = list(filter(lambda s: s.name == step, steps))[0]

        jobs_path = _fabricks.resolve_runtime_path() / _step.path_options.runtime
        jobs_generator: Generator[SilverJobConfig, None, None] = SilverJobConfig.from_files(
            jobs_path, root="job", preferred_file_name=topic
        )
        jobs: List[SilverJobConfig] = list(jobs_generator)
        job: SilverJobConfig = list(filter(lambda j: j.topic == topic and j.item == item, jobs))[0]

        return cls(fabricks=_fabricks, runtime=runtime, step=_step, job=job)

    @classmethod
    def from_parts(
        cls,
        fabricks: FabricksConfig,
        runtime: RuntimeConfig,
        step: SilverStepConfig,
        job: SilverJobConfig,
    ) -> "SilverJobConfigResolver":
        return cls(fabricks=fabricks, runtime=runtime, step=step, job=job)
