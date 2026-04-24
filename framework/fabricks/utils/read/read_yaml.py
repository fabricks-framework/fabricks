from functools import lru_cache
from typing import Iterable, Optional, cast

import yaml

from fabricks.utils.path import BasePath


@lru_cache(maxsize=128)
def _read_yaml_cached(file: str) -> list[dict]:
    """Cache YAML file reads with LRU eviction. Max 128 unique file paths cached."""
    with open(file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_yaml(
    path: BasePath,
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

        data = _read_yaml_cached(file)
        for job_config in data:
            if root:
                yield cast(dict, job_config[root])
            else:
                yield cast(dict, job_config)

    if preferred_file_name is not None and not found:
        yield from read_yaml(path=path, root=root, preferred_file_name=None)
