from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ModelBase(BaseModel):
    # Ignore extra/unknown fields (TypedDict-like strictness)
    model_config = ConfigDict(extra="ignore")
