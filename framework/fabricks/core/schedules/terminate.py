import warnings

from fabricks.core.schedules.dags import terminate

warnings.warn(
    "Importing from fabricks.core.schedules.terminate is deprecated. "
    "Import from fabricks.core.schedules.dags instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["terminate"]
