import warnings

from fabricks.core.schedules.dags import generate

warnings.warn(
    "Importing from fabricks.core.schedules.generate is deprecated. Import from fabricks.core.schedules.dags instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["generate"]
