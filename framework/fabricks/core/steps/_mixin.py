from typing import Optional

from typing_extensions import deprecated

from fabricks.core.steps._protocol import StepProtocol


class LegacyStepMixin(StepProtocol):
    """
    Backward-compat aliases for _internal methods removed in refactor.
    Mix in before BaseStep: class MyStep(LegacyStepMixin, BaseStep).

    Behavioral note: _create_db_objects_internal and _update_dependencies_internal
    now raise on error instead of returning (df, errors). Callers that inspected
    the error list must switch to the public methods directly.
    """

    @deprecated("use create_db_objects instead")
    def create_jobs(self, max_retries: Optional[int] = 2) -> None:
        return self.create_db_objects(max_retries=max_retries)

    @deprecated("use update_configurations instead")
    def update_jobs(self, drop: Optional[bool] = False):
        return self.update_configurations(drop=drop)

    @deprecated("use update_tables_list instead")
    def update_tables(self):
        return self.update_tables_list()

    @deprecated("use update_views_list instead")
    def update_views(self):
        return self.update_views_list()
