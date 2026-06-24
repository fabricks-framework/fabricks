from __future__ import annotations

from pyspark.sql import DataFrame

from fabricks.models.cdc import CDCIntentContext


def apply_memory_mode(context: CDCIntentContext, mode: str) -> None:
    """Override CDC mode to 'complete' for memory/view jobs."""
    if mode == "memory":
        context["mode"] = "complete"


def apply_scd_key_flags(
    context: CDCIntentContext,
    slowly_changing_dimension: bool,
    df: DataFrame,
) -> None:
    """Add add_key / add_hash when SCD and the columns are absent from the DataFrame."""
    if slowly_changing_dimension:
        if "__key" not in df.columns:
            context["add_key"] = True
        if "__hash" not in df.columns:
            context["add_hash"] = True


def apply_correct_valid_from(
    context: CDCIntentContext,
    change_data_capture: str,
    value: bool = True,
) -> None:
    """Set correct_valid_from for scd2 jobs."""
    if change_data_capture == "scd2":
        context["correct_valid_from"] = value
