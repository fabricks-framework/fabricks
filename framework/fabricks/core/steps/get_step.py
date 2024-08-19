from typing import Union

from framework.fabricks.core.jobs.base.types import Steps, TStep
from framework.fabricks.core.steps.base import BaseStep


def get_step(step: Union[TStep, str]) -> BaseStep:
    assert step in Steps, f"{step} not found"
    _step = BaseStep(step=step)
    return _step
