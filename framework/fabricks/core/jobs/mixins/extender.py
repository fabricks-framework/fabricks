from pyspark.sql import DataFrame

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.extenders import get_extender
from fabricks.core.jobs.mixins._protocol import JobProtocol
from fabricks.models.common import ExtenderOptions


class ExtenderMixin(JobProtocol):
    def extend_job(self, df: DataFrame) -> DataFrame:
        extenders = self.extender_options or []
        return self._extend(df, extenders, extended="job")

    def extend_step(self, df: DataFrame) -> DataFrame:
        extenders = self.step_conf.extender_options or []
        return self._extend(df, extenders, extended="step")

    def _extend(self, df: DataFrame, extenders: list[ExtenderOptions], extended: str) -> DataFrame:
        for e in extenders:
            name = e.extender
            DEFAULT_LOGGER.debug(f"extend {extended} ({name})", extra={"label": self})
            arguments = e.arguments or {}
            extender = get_extender(name)
            df = extender(df, **arguments)

        return df

    def extend(self, df: DataFrame) -> DataFrame:
        df = self.extend_job(df)
        df = self.extend_step(df)

        return df
