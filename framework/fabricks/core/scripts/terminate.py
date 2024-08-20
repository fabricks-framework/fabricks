from fabricks.core.dags.terminator import DagTerminator


def terminate(schedule_id: str):
    with DagTerminator(schedule_id=schedule_id) as t:
        t.terminate()
