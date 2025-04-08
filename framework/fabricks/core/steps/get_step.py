from typing import Union

from fabricks.core.jobs.base._types import Steps, TStep
from fabricks.core.steps.base import BaseStep


def get_step(step: Union[TStep, str]) -> BaseStep:
    assert step in Steps, f"{step} not found"
    base_step = BaseStep(step=step)
    return base_step
