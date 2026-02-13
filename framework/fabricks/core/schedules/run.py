import warnings

from fabricks.core.dags.run import run

warnings.warn(
    "Importing from fabricks.core.schedules.run is deprecated. Import from fabricks.core.schedules.dags instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["run"]
