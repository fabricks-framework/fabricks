from dataclasses import dataclass
from typing import List

from fabricks.core.jobs.base._types import JobConf
from fabricks.utils.schema import get_json_schema_for_type


def get_job_schema() -> str:
    import json

    @dataclass
    class JobWrapper:
        job: JobConf

    sc = get_json_schema_for_type(List[JobWrapper])
    defs: dict[str, dict] = sc["$defs"]
    removals = [("Job", "job_id"), ("Job", "table")]

    for key, defi in defs.items():
        for ent, prop in removals:
            if key.startswith(ent) and prop in defi["properties"]:
                req: List[str] = defi["required"]
                req.remove(prop)  # not defined in yaml
                jobprops: dict = defi["properties"]
                jobprops.pop(prop)

    j = json.dumps(sc, indent=4)
    return j
