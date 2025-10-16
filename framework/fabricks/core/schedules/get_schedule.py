from pyspark.sql.types import Row

from fabricks.core.schedules.get_schedules import get_schedules


def get_schedule(name: str) -> Row:
    schedule = next(s for s in get_schedules() if s.get("name") == name)

    assert schedule, "schedule not found"
    assert len(schedule) == 1, "schedule duplicated"
    return Row(**schedule)
