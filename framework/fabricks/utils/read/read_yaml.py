from typing import Iterable, Optional, cast

import yaml

from fabricks.utils.path import Path


def read_yaml(
    path: Path,
    root: Optional[str] = None,
    preferred_file_name: Optional[str] = None,
) -> Iterable[dict]:
    found = False

    for file in path.walk():
        if not file.endswith(".yml"):
            continue

        if preferred_file_name is not None and preferred_file_name not in file:
            continue

        found = True

        with open(file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            for job_config in data:
                if root:
                    yield cast(dict, job_config[root])
                else:
                    yield cast(dict, job_config)

    if preferred_file_name is not None and not found:
        yield from read_yaml(path=path, root=root, preferred_file_name=None)
