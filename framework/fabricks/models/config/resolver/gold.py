from typing import Generator, List

from fabricks.models.config.fabricks import FabricksConfig
from fabricks.models.config.jobs.gold import GoldJobConfig
from fabricks.models.config.resolver.base import BaseConfigResolver
from fabricks.models.config.runtime import RuntimeConfig
from fabricks.models.config.steps.gold import GoldStepConfig


class GoldConfigResolver(BaseConfigResolver):
    step: GoldStepConfig
    job: GoldJobConfig

    @classmethod
    def from_file(cls, step: str, topic: str, item: str) -> "GoldConfigResolver":
        _fabricks = FabricksConfig.load()

        runtime_path = _fabricks.resolve_config_path()
        runtime_generator: Generator[RuntimeConfig, None, None] = RuntimeConfig.from_file(runtime_path, root="conf")
        runtime = next(runtime_generator)

        if runtime.gold is None:
            raise ValueError("gold not found in runtime configuration")

        steps: List[GoldStepConfig] = runtime.gold
        _step: GoldStepConfig = list(filter(lambda s: s.name == step, steps))[0]

        jobs_path = _fabricks.resolve_runtime_path() / _step.path_options.runtime
        jobs_generator: Generator[GoldJobConfig, None, None] = GoldJobConfig.from_files(jobs_path, root="job")
        jobs: List[GoldJobConfig] = list(jobs_generator)
        job: GoldJobConfig = list(filter(lambda j: j.topic == topic and j.item == item, jobs))[0]

        return cls(fabricks=_fabricks, runtime=runtime, step=_step, job=job)

    @classmethod
    def from_parts(
        cls,
        fabricks: FabricksConfig,
        runtime: RuntimeConfig,
        step: GoldStepConfig,
        job: GoldJobConfig,
    ) -> "GoldConfigResolver":
        return cls(fabricks=fabricks, runtime=runtime, step=step, job=job)
