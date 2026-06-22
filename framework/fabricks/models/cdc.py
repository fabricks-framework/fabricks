from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, TypedDict


@dataclass(frozen=True)
class CDCQueryContext:
    # identity
    template: str
    debugmode: bool
    src: Any  # AllowedSources: DataFrame | Table | str
    format: str  # "dataframe" | "table" | "query"
    tgt: Optional[str]
    cdc: str  # "nocdc" | "scd0" | "scd1" | "scd2"
    mode: str  # "complete" | "update" | "append"
    # column sets
    inputs: list[str]
    fields: list[str]
    intermediates: list[str]
    outputs: list[str]
    keys: Optional[list[str]]
    hashes: Optional[list[str]]
    # column presence
    has_operation: bool
    has_metadata: bool
    has_source: bool
    has_timestamp: bool
    has_key: bool
    has_hash: bool
    has_identity: bool
    has_rescued_data: bool
    has_last_updated: bool
    has_rows: Optional[bool]
    has_order_by: Optional[bool]
    has_no_data: Optional[bool]
    # CTE switches
    slice: Optional[str]
    rectify: Optional[bool]
    deduplicate: Optional[bool]
    deduplicate_key: Optional[bool]
    deduplicate_hash: Optional[bool]
    advanced_deduplication: Optional[bool]
    # options
    delete_missing: Optional[bool]
    soft_delete: Optional[bool]
    correct_valid_from: Optional[bool]
    # add-column directives
    add_operation: Any  # str ("upsert") | bool | None
    add_source: Any
    add_calculated_columns: list[str]
    add_key: Optional[bool]
    add_hash: Optional[bool]
    add_timestamp: Optional[bool]
    add_last_updated: Optional[bool]
    add_metadata: Optional[bool]
    # extra
    order_duplicate_by: Optional[list[str]]
    overwrite: list[str]
    cast: dict[str, str]
    # CTE parents (derived from switches; all required — computed by get_query_context)
    parent_slice: Optional[str]
    parent_rectify: Optional[str]
    parent_deduplicate_key: Optional[str]
    parent_deduplicate_hash: Optional[str]
    parent_cdc: str
    parent_final: str
    # filter (None until populated by fix_context via dataclasses.replace)
    slices: Optional[Any] = None
    sources: Optional[Any] = None
    filter_where: Optional[str] = None
    update_where: Optional[str] = None


class CDCIntentContext(TypedDict, total=False):
    """Typed kwargs passed from a job's get_cdc_context() into CDC operations.

    All keys match kwargs consumed by CDCQueryContext / get_query_context().
    Using total=False because jobs supply only the keys relevant to their layer.
    """

    mode: str
    soft_delete: Optional[bool]
    deduplicate: Optional[bool]
    deduplicate_key: Optional[bool]
    deduplicate_hash: Optional[bool]
    rectify: Optional[bool]
    order_duplicate_by: Optional[dict[str, str]]
    slice: Optional[str]
    correct_valid_from: Optional[bool]
    add_key: Optional[bool]
    add_hash: Optional[bool]
    add_operation: Any
    add_timestamp: Optional[bool]
    add_last_updated: Optional[bool]
    add_metadata: Optional[bool]
    exclude: Optional[list[str]]
    delete_missing: Optional[bool]


@dataclass(frozen=True)
class CDCMergeContext:
    template: str
    debugmode: bool
    src: Any  # DataFrame | str (view name)
    format: str  # "dataframe" | "view"
    tgt: Any  # Table
    cdc: str
    columns: list[str]
    fields: list[str]
    soft_delete: bool
    has_source: bool
    has_identity: bool
    has_key: bool
    has_hash: bool
    keys: Optional[list[str]]
    has_metadata: bool
    has_timestamp: bool
    where: Optional[str]
