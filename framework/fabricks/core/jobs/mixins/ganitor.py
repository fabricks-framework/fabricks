from typing import Optional

from typing_extensions import deprecated

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.mixins._protocol import JobProtocol


class GanitorMixin(JobProtocol):
    @deprecated("use maintain instead")
    def optimize(
        self,
        vacuum: Optional[bool] = True,
        optimize: Optional[bool] = True,
        analyze: Optional[bool] = True,
    ):
        return self.maintain(vacuum=vacuum, optimize=optimize, compute_statistics=analyze)

    def maintain(
        self,
        vacuum: Optional[bool] = True,
        optimize: Optional[bool] = True,
        compute_statistics: Optional[bool] = True,
    ):
        if self.mode == "memory":
            DEFAULT_LOGGER.debug("could not maintain (memory)", extra={"label": self})
        else:
            if vacuum:
                self.vacuum()
            if optimize:
                self.cdc.optimize_table()
            if compute_statistics:
                self.table.compute_statistics()

    def vacuum(self):
        if self.mode == "memory":
            DEFAULT_LOGGER.debug("could not vacuum (memory)", extra={"label": self})
        else:
            job = self.table_options.retention_days if self.table_options else None
            step = self.step_table_options.retention_days if self.step_table_options else None
            runtime = self.runtime_options.retention_days
            if job is not None:
                retention_days = job
            elif step:
                retention_days = step
            else:
                assert runtime
                retention_days = runtime
            self.table.vacuum(retention_days=retention_days)
