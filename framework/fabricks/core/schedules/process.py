from fabricks.core.dags.processor import DagProcessor


def process(schedule_id: str, schedule: str, step: str):
    with DagProcessor(schedule_id=schedule_id, schedule=schedule, step=step) as p:
        p.process()
