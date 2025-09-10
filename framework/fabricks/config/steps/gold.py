from __future__ import annotations

from typing import Optional

from fabricks.config.steps.base import BaseStepConfig, DefaultOptions


class GoldOptions(DefaultOptions):
    schema_drift: Optional[bool] = None
    metadata: Optional[bool] = None


class GoldStepConfig(BaseStepConfig):
    options: GoldOptions
