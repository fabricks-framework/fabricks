"""Shared test doubles for the spark/config tier.

`_FakeDF` stands in for a DataFrame wherever only `.columns`/`.dtypes` are
accessed (no isinstance(..., DataFrameLike) check involved) - pure Python,
no Spark needed. Used by test_column_selection.py, test_cdc_context.py and
test_create_table_defaults.py.
"""

from dataclasses import dataclass, field
from unittest.mock import MagicMock


@dataclass
class _FakeDF:
    columns: list[str]
    dtypes: list[tuple[str, str]] = field(default_factory=list)

    def createOrReplaceGlobalTempView(self, _name: str) -> None:  # noqa: N802 - matches pyspark's DataFrame API
        """No-op: Silver.build_cdc_context() registers a global temp view unconditionally."""


def stub_table(monkeypatch, job) -> None:
    """job.run() reads table state up front; no real Spark in this tier."""
    table = MagicMock(get_last_version=lambda: 0, get_property=lambda _k: None)
    monkeypatch.setattr(type(job), "table", table)
