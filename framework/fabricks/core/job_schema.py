import json
from typing import List

from pydantic import BaseModel, TypeAdapter

from fabricks.models import JobConf, JobConfBronze, JobConfGold, JobConfSilver


class JobWrapper(BaseModel):
    """Wrapper for JobConf to generate array schema."""

    job: JobConf


class BronzeJobWrapper(JobWrapper):
    job: JobConfBronze


class SilverJobWrapper(JobWrapper):
    job: JobConfSilver


class GoldJobWrapper(JobWrapper):
    job: JobConfGold


def get_job_schema(step: str | None = None) -> str:
    if step == "bronze":
        wrapper = BronzeJobWrapper
    elif step == "silver":
        wrapper = SilverJobWrapper
    elif step == "gold":
        wrapper = GoldJobWrapper
    else:
        wrapper = JobWrapper

    # Use List[JobWrapper] to create the array schema
    adapter = TypeAdapter(List[wrapper])
    sc = adapter.json_schema()

    # Remove properties that are not defined in YAML
    defs: dict[str, dict] = sc.get("$defs", {})
    removals = [("Job", "job_id"), ("Job", "table")]

    for key, defi in defs.items():
        for ent, prop in removals:
            if key.startswith(ent) and prop in defi.get("properties", {}):
                req: List[str] = defi.get("required", [])
                if prop in req:
                    req.remove(prop)  # not defined in yaml

                jobprops: dict = defi.get("properties", {})
                jobprops.pop(prop, None)

    return json.dumps(sc, indent=4)


def print_job_schema(step: str | None = None) -> None:
    print(get_job_schema(step))
