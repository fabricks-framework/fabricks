from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class CdcContext(BaseModel):
    model_config = ConfigDict(extra="ignore")
    keys: Optional[list[str]] = None
    mode: str = "complete"
    exclude: list[str] = Field(default_factory=list)
    cast: dict[str, Any] = Field(default_factory=dict)
    order_duplicate_by: Optional[dict[str, str]] = None
    add_source: Optional[bool] = None
    add_calculated_columns: list = Field(default_factory=list)
    add_operation: Optional[str] = None
    add_key: Optional[bool] = None
    add_hash: Optional[bool] = None
    add_timestamp: Optional[bool] = None
    add_last_updated: Optional[bool] = None
    add_metadata: Optional[bool] = None
    soft_delete: Optional[bool] = None
    delete_missing: Optional[bool] = None
    slice: Optional[str] = None
    rectify: Optional[bool] = None
    deduplicate: Optional[bool] = None
    deduplicate_key: Optional[bool] = None
    deduplicate_hash: Optional[bool] = None
    correct_valid_from: Optional[bool] = None
    filter_where: Optional[str] = None
    update_where: Optional[str] = None
    uuid: bool = False
    schema_drift: bool = False


class QueryContext(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    template: Literal["filter", "merger", "query"]
    debugmode: bool
    src: Any
    format: str
    tgt: Optional[str]
    cdc: str
    mode: str
    inputs: list[str]
    intermediates: list[str]
    outputs: list[str]
    fields: list[str]
    keys: Optional[list[str]]
    hashes: Optional[list[str]]
    delete_missing: Optional[bool]
    advanced_deduplication: Any
    slice: Optional[str]
    rectify: Any
    deduplicate: Any
    deduplicate_key: Any
    deduplicate_hash: Any
    has_no_data: Optional[bool]
    has_rows: Optional[bool]
    has_source: Any
    has_metadata: Any
    has_last_updated: Any
    has_timestamp: Any
    has_operation: Any
    has_identity: bool
    has_key: Any
    has_hash: Any
    has_order_by: Optional[bool]
    has_rescued_data: bool
    add_metadata: Optional[bool]
    add_timestamp: Optional[bool]
    add_last_updated: Optional[bool]
    add_key: Optional[bool]
    add_hash: Optional[bool]
    add_operation: Optional[str]
    add_source: Optional[bool]
    add_calculated_columns: list
    order_duplicate_by: Optional[list[str]]
    soft_delete: Optional[bool]
    correct_valid_from: Any
    overwrite: list[str]
    cast: dict[str, Any]
    slices: Optional[Any] = None
    sources: Optional[Any] = None
    filter_where: Optional[str]
    update_where: Optional[str]
    parent_slice: Optional[str]
    parent_rectify: Optional[str]
    parent_deduplicate_key: Optional[str]
    parent_deduplicate_hash: Optional[str]
    parent_cdc: str
    parent_final: str
