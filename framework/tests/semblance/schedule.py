"""Row builders for the dependency and status partitions DagGenerator writes (fabricks/core/dags/generator.py)."""


def status_row(
    job_id: str,
    *,
    status: str = "scheduled",
    step: str = "silver",
    job: str | None = None,
    rank: int = 1,
    schedule_id: str = "s1",
    schedule: str = "daily",
) -> dict:
    return {
        "PartitionKey": "statuses",
        "RowKey": job_id,
        "ScheduleId": schedule_id,
        "Schedule": schedule,
        "Step": step,
        "JobId": job_id,
        "Job": job or job_id,
        "Status": status,
        "Rank": rank,
    }


def dependency_row(
    job_id: str,
    parent_id: str,
    *,
    status: str = "pending",
    step: str = "silver",
    job: str | None = None,
    parent_step: str = "silver",
    parent: str | None = None,
    schedule_id: str = "s1",
    schedule: str = "daily",
) -> dict:
    dependency_id = f"{job_id}:{parent_id}"
    return {
        "PartitionKey": "dependencies",
        "RowKey": dependency_id,
        "DependencyId": dependency_id,
        "ScheduleId": schedule_id,
        "Schedule": schedule,
        "Step": step,
        "Job": job or job_id,
        "JobId": job_id,
        "ParentStep": parent_step,
        "Parent": parent or parent_id,
        "ParentId": parent_id,
        "Status": status,
    }
