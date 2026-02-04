from typing import List

from pydantic import BaseModel

from fabricks.models import JobConf


class JobWrapper(BaseModel):
    """Wrapper for JobConf to generate array schema."""

    job: JobConf


def get_job_schema() -> str:
    import json

    # Generate JSON schema using Pydantic's built-in method
    # Use List[JobWrapper] to create the array schema
    from pydantic import TypeAdapter

    adapter = TypeAdapter(List[JobWrapper])
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


def print_job_schema():
    print(get_job_schema())
