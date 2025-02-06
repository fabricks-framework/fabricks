from typing import Union

from fabricks.core.jobs.base._types import Steps, TStep
from fabricks.core.steps.base import BaseStep


def get_step(step: Union[TStep, str]) -> BaseStep:
    assert step in Steps, f"{step} not found"
    _step = BaseStep(step=step)
    return _step
