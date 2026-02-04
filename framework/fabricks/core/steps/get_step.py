from fabricks.context import Steps
from fabricks.core.steps.base import BaseStep


def get_step(step: str) -> BaseStep:
    assert step in Steps, f"{step} not found"
    base_step = BaseStep(step=step)
    return base_step
