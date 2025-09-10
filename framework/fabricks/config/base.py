from __future__ import annotations

from pathlib import Path
from typing import Optional, cast

import yaml
from pydantic import BaseModel, ConfigDict


class ModelBase(BaseModel):
    # Ignore extra/unknown fields (TypedDict-like strictness)
    model_config = ConfigDict(extra="ignore")

    @classmethod
    def from_yaml(cls, path: Path, root: Optional[str] = None):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            for job_config in data:
                if root:
                    return cls(**cast(dict, job_config[root]))
                else:
                    return cls(**cast(dict, job_config))

    @classmethod
    def from_yamls(cls, path: Path, root: Optional[str] = None):
        for child in path.iterdir():
            if child.suffix == ".yaml" or child.suffix == ".yml":
                yield cls.from_yaml(child, root=root)
            elif child.is_dir():
                yield from cls.from_yamls(child, root=root)
