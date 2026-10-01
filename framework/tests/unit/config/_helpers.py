"""Shared test doubles for the spark/config tier.

`_FakeDF` stands in for a DataFrame wherever only `.columns`/`.dtypes` are
accessed (no isinstance(..., DataFrameLike) check involved) - pure Python,
no Spark needed. Used by test_column_selection.py, test_cdc_context.py and
test_create_table_defaults.py.
"""

from dataclasses import dataclass, field
from unittest.mock import MagicMock

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row


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


def fake_spark() -> MagicMock:
    return MagicMock(name="fake_spark", spec=SparkSession)


def src(columns: list[str], *, is_empty: bool | None = None) -> MagicMock:
    """A DataFrame-shaped mock with `.columns`. `is_empty` sets `.isEmpty()`; left unset it stays a truthy
    MagicMock, which Processor.has_data() reads as "no data" (see get_query_context's issue #182 guard)."""
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    if is_empty is not None:
        df.isEmpty.return_value = is_empty
    return df


def probe_spark(
    *, rows: int = 10, slices: str = "( s.__timestamp > '2024-01-01' )", sources: str | None = "t.__source == 'a'"
) -> MagicMock:
    """A fake Spark that answers the two queries `get_query_context` issues: `select count(*)` returns `rows`,
    and the incremental-filter probe (the statement that selects `slices`) returns one slices/sources row."""
    spark = fake_spark()

    def _sql(sql: str) -> MagicMock:
        result = MagicMock()
        lowered = sql.strip().lower()
        if lowered.startswith("select count(*)"):
            result.collect.return_value = [[rows]]
        elif "slices" in lowered:
            result.collect.return_value = [Row(slices=slices, sources=sources)]
        return result

    spark.sql.side_effect = _sql
    return spark


def sql_statements(spark: MagicMock) -> list[str]:
    return [c.args[0] for c in spark.sql.call_args_list]
