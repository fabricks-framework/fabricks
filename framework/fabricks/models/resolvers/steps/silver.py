from typing import Generator, List

from fabricks.models.config.fabricks import FabricksConfig
from fabricks.models.config.runtime import RuntimeConfig
from fabricks.models.config.steps.silver import SilverStepConfig
from fabricks.models.resolvers.steps.base import BaseStepConfigResolver


class SilverStepConfigResolver(BaseStepConfigResolver):
    step: SilverStepConfig
    jobs: List[SilverStepConfig]

    @classmethod
    def from_file(cls, step: str) -> "SilverStepConfigResolver":
        _fabricks = FabricksConfig.load()

        _fabricks = FabricksConfig.load()

        runtime_path = _fabricks.resolve_config_path()
        runtime_generator: Generator[RuntimeConfig, None, None] = RuntimeConfig.from_file(runtime_path, root="conf")
        runtime = next(runtime_generator)

        if runtime.silver is None:
            raise ValueError("silver not found in runtime configuration")

        steps: List[SilverStepConfig] = runtime.silver
        _step: SilverStepConfig = list(filter(lambda s: s.name == step, steps))[0]

        jobs_path = _fabricks.resolve_runtime_path() / _step.path_options.runtime
        jobs_generator: Generator[SilverStepConfig, None, None] = SilverStepConfig.from_files(jobs_path, root="job")
        jobs: List[SilverStepConfig] = list(jobs_generator)

        return cls(fabricks=_fabricks, runtime=runtime, step=_step, jobs=jobs)

    @classmethod
    def from_parts(
        cls,
        fabricks: FabricksConfig,
        runtime: RuntimeConfig,
        step: SilverStepConfig,
        jobs: List[SilverStepConfig],
    ) -> "SilverStepConfigResolver":
        return cls(fabricks=fabricks, runtime=runtime, step=step, jobs=jobs)
