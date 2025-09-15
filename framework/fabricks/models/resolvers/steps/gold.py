from typing import Generator, List

from fabricks.models.config.fabricks import FabricksConfig
from fabricks.models.config.runtime import RuntimeConfig
from fabricks.models.config.steps.gold import GoldStepConfig
from fabricks.models.resolvers.steps.base import BaseStepConfigResolver


class GoldStepConfigResolver(BaseStepConfigResolver):
    step: GoldStepConfig
    jobs: List[GoldStepConfig]

    @classmethod
    def from_file(cls, step: str) -> "GoldStepConfigResolver":
        _fabricks = FabricksConfig.load()

        _fabricks = FabricksConfig.load()

        runtime_path = _fabricks.resolve_config_path()
        runtime_generator: Generator[RuntimeConfig, None, None] = RuntimeConfig.from_file(runtime_path, root="conf")
        runtime = next(runtime_generator)

        if runtime.gold is None:
            raise ValueError("gold not found in runtime configuration")

        steps: List[GoldStepConfig] = runtime.gold
        _step: GoldStepConfig = list(filter(lambda s: s.name == step, steps))[0]

        jobs_path = _fabricks.resolve_runtime_path() / _step.path_options.runtime
        jobs_generator: Generator[GoldStepConfig, None, None] = GoldStepConfig.from_files(jobs_path, root="job")
        jobs: List[GoldStepConfig] = list(jobs_generator)

        return cls(fabricks=_fabricks, runtime=runtime, step=_step, jobs=jobs)

    @classmethod
    def from_parts(
        cls,
        fabricks: FabricksConfig,
        runtime: RuntimeConfig,
        step: GoldStepConfig,
        jobs: List[GoldStepConfig],
    ) -> "GoldStepConfigResolver":
        return cls(fabricks=fabricks, runtime=runtime, step=step, jobs=jobs)
