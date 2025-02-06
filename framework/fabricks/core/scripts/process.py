from typing import Union

from fabricks.core.dags.processor import DagProcessor
from fabricks.core.jobs.base._types import TStep


def process(schedule_id: str, schedule: str, step: Union[TStep, str]):
    with DagProcessor(schedule_id=schedule_id, schedule=schedule, step=step) as p:
        p.process()
