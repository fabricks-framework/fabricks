import warnings

from fabricks.core.schedules.dags import process

warnings.warn(
    "Importing from fabricks.core.schedules.process is deprecated. Import from fabricks.core.schedules.dags instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["process"]
