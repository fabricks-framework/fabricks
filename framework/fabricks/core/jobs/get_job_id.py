from typing import Optional, overload

from fabricks.utils.helpers import md5


@overload
def get_job_id(step: str, topic: str, item: str) -> str: ...


@overload
def get_job_id(*, job: str) -> str: ...


def get_job_id(
    step: Optional[str] = None,
    topic: Optional[str] = None,
    item: Optional[str] = None,
    job: Optional[str] = None,
) -> str:
    if not job:
        assert step
        assert topic
        assert item
        job = f"{step}.{topic}_{item}"

    return md5(job)
