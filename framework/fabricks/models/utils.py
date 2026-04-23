"""Utility functions for job and dependency ID generation."""

from typing import Any, Optional, overload


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


def get_dependency_id(parent: str, job_id: str) -> str:
    base = f"{job_id}*{parent}"
    return md5(base)


def md5(s: Any) -> str:
    from hashlib import md5

    hash_obj = md5(str(s).encode())
    return hash_obj.hexdigest()
