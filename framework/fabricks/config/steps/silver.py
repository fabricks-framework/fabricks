from __future__ import annotations

from typing import Optional

from fabricks.config.steps.base import BaseStepConfig, DefaultOptions


class SilverOptions(DefaultOptions):
    parent: str
    stream: Optional[bool] = None
    local_checkpoint: Optional[bool] = None


class SilverStepConfig(BaseStepConfig):
    options: SilverOptions
