from typing import Union

from framework.fabricks.core.dags.processor import DagProcessor
from framework.fabricks.core.jobs.base.types import TStep


def process(schedule_id: str, schedule: str, step: Union[TStep, str]):
    with DagProcessor(schedule_id=schedule_id, schedule=schedule, step=step) as p:
        p.process()
