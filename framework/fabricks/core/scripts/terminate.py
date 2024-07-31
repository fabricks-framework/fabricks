from fabricks.core.dags.terminator import DagTerminator


def terminate(schedule_id: str):
    t = DagTerminator(schedule_id=schedule_id)
    t.terminate()
