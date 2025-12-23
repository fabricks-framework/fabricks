from typing import Union

from fabricks.context import Steps
from fabricks.core.steps.base import BaseStep
from fabricks.models import TStep


def get_step(step: Union[TStep, str]) -> BaseStep:
    assert step in Steps, f"{step} not found"
    base_step = BaseStep(step=step)
    return base_step
