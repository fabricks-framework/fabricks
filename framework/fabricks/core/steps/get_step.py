from fabricks.context import Steps
from fabricks.core.steps.base import BaseStep


def get_step(step: str) -> BaseStep:
    assert step in Steps, f"{step} not found"
    return BaseStep(step=step)
