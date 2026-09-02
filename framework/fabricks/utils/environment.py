# framework/fabricks/utils/environment.py
import os
from typing import Final, Literal

FabricksEnvironment = Literal["docker", "remote", "databricks"]

_ENVIRONMENTS: tuple[FabricksEnvironment, ...] = ("docker", "remote", "databricks")
_raw = os.getenv("FABRICKS_ENVIRONMENT", "databricks").lower()
assert _raw in _ENVIRONMENTS, f"FABRICKS_ENVIRONMENT must be one of {_ENVIRONMENTS}, got {_raw!r}"
FABRICKS_ENVIRONMENT: Final[FabricksEnvironment] = _raw  # type: ignore[assignment]
