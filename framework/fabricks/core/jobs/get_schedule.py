from typing import Any, Dict

from fabricks.core.jobs.get_schedules import get_schedules


def get_schedule(name: str) -> Dict[str, Any]:
    schedule = next(s for s in get_schedules() if s.get("name") == name)

    assert schedule, "schedule not found"
    return schedule
