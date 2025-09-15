from typing import Generator, List

from fabricks.models.config.fabricks import FabricksConfig
from fabricks.models.config.runtime import RuntimeConfig
from fabricks.models.config.steps.bronze import BronzeStepConfig
from fabricks.models.resolvers.steps.base import BaseStepConfigResolver


class BronzeStepConfigResolver(BaseStepConfigResolver):
    step: BronzeStepConfig
    jobs: List[BronzeStepConfig]

    @classmethod
    def from_file(cls, step: str) -> "BronzeStepConfigResolver":
        _fabricks = FabricksConfig.load()

        _fabricks = FabricksConfig.load()

        runtime_path = _fabricks.resolve_config_path()
        runtime_generator: Generator[RuntimeConfig, None, None] = RuntimeConfig.from_file(runtime_path, root="conf")
        runtime = next(runtime_generator)

        if runtime.bronze is None:
            raise ValueError("bronze not found in runtime configuration")

        steps: List[BronzeStepConfig] = runtime.bronze
        _step: BronzeStepConfig = list(filter(lambda s: s.name == step, steps))[0]

        jobs_path = _fabricks.resolve_runtime_path() / _step.path_options.runtime
        jobs_generator: Generator[BronzeStepConfig, None, None] = BronzeStepConfig.from_files(jobs_path, root="job")
        jobs: List[BronzeStepConfig] = list(jobs_generator)

        return cls(fabricks=_fabricks, runtime=runtime, step=_step, jobs=jobs)

    @classmethod
    def from_parts(
        cls,
        fabricks: FabricksConfig,
        runtime: RuntimeConfig,
        step: BronzeStepConfig,
        jobs: List[BronzeStepConfig],
    ) -> "BronzeStepConfigResolver":
        return cls(fabricks=fabricks, runtime=runtime, step=step, jobs=jobs)
