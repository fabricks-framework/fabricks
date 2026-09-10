"""Generator.create_table()'s own resolution branches (framework/fabricks/
core/jobs/base/generator.py:267-374), distinct from the already-tested
Table._create() DDL mapping (test_ddl_option_mapping.py): default
`properties` by powerbi/maximum_compatibility, identity/liquid_clustering
resolution, the `__generated_*` dunder-prefix assertion, and primary_key/
foreign_keys passthrough.

self.get_data/base_transform/get_cdc_context and job.cdc.create_table are
all monkeypatched so the real chain (a real .sql fixture file, real
CDC-context derivation, real DDL rendering - all already covered by other
tests in this tier/test_ddl_option_mapping.py) never runs; only
create_table()'s own kwargs-assembly is under test, captured off
job.cdc.create_table's call kwargs.
"""

import pytest

from fabricks.core import get_job
from fabricks.models.table import TableOptions
from tests.unit.config._helpers import _FakeDF


class _FakeCreateTableDF(_FakeDF):
    def limit(self, _n: int) -> "_FakeCreateTableDF":
        return self

    def createOrReplaceGlobalTempView(self, _name: str) -> None:  # noqa: N802 - matches pyspark's DataFrame API
        pass


def _job(monkeypatch, *, table_options: TableOptions | None = None, columns: list[str] | None = None):
    job = get_job(step="gold", topic="fact", item="step_option")

    # The "gold" step declares table_options.properties of its own
    # (tests/spark/apache/runtime/fabricks/conf.fabricks.yml) - option-level
    # fallback onto that is test_option_hierarchy.py's concern, not this
    # file's. Clear it so create_table()'s own default-properties branch is
    # what's under test here, isolated from the job/step hierarchy.
    job.base_step_conf = job.step_conf.model_copy(update={"table_options": None})

    if table_options is not None:
        job.conf = job.conf.model_copy(update={"table_options": table_options})

    columns = columns or ["id", "name"]
    df = _FakeCreateTableDF(
        columns=columns,
        dtypes=[(column, "int" if column == "id" else "string") for column in columns],
    )
    monkeypatch.setattr(job, "get_data", lambda **_kwargs: df)
    monkeypatch.setattr(job, "base_transform", lambda d: d)
    monkeypatch.setattr(job, "get_cdc_context", lambda _d: {})
    # register_udfs() (unpatched) would read self.sql -> a real runtime .sql
    # fixture file step_option has none of, since it exists only for the
    # option-hierarchy/timeout/check tests - out of scope here.
    monkeypatch.setattr(job, "register_udfs", lambda **_kwargs: None)
    monkeypatch.setattr(job.table, "exists", lambda: False)

    captured = {}
    monkeypatch.setattr(job.cdc, "create_table", lambda _sql, **kwargs: captured.update(kwargs))

    return job, captured


def test_create_table_default_properties(monkeypatch):
    job, captured = _job(monkeypatch)

    job.create_table()

    assert captured["properties"] == {
        "delta.enableTypeWidening": "true",
        "delta.enableDeletionVectors": "true",
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
        "delta.feature.timestampNtz": "supported",
        "fabricks.last_version": "0",
    }


def test_create_table_powerbi_properties(monkeypatch):
    job, captured = _job(monkeypatch, table_options=TableOptions(powerbi=True))

    job.create_table()

    assert captured["properties"] == {
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
        "fabricks.last_version": "0",
    }


def test_create_table_maximum_compatibility_properties(monkeypatch):
    job, captured = _job(monkeypatch, table_options=TableOptions(maximum_compatibility=True))

    job.create_table()

    assert captured["properties"] == {
        "delta.minReaderVersion": "1",
        "delta.minWriterVersion": "7",
        "delta.columnMapping.mode": "none",
        "fabricks.last_version": "0",
    }


def test_create_table_explicit_properties_win_over_defaults(monkeypatch):
    job, captured = _job(monkeypatch, table_options=TableOptions(properties={"delta.appendOnly": "true"}))

    job.create_table()

    assert captured["properties"] == {"delta.appendOnly": "true"}


def test_create_table_liquid_clustering_auto(monkeypatch):
    job, captured = _job(
        monkeypatch,
        table_options=TableOptions(liquid_clustering=True),
        columns=["id", "name", "__cluster_by_id"],
    )

    job.create_table()

    assert captured["liquid_clustering"] is True
    assert captured["cluster_by"] == ["__cluster_by_id"]


def test_create_table_liquid_clustering_explicit_list(monkeypatch):
    job, captured = _job(monkeypatch, table_options=TableOptions(cluster_by=["monarch"]))

    job.create_table()

    assert captured["liquid_clustering"] is True
    assert captured["cluster_by"] == ["monarch"]


def test_create_table_liquid_clustering_disabled_when_option_false(monkeypatch):
    job, captured = _job(monkeypatch, table_options=TableOptions(liquid_clustering=False))

    job.create_table()

    assert captured["liquid_clustering"] is False


def test_create_table_generated_columns_dunder_prefix_required(monkeypatch):
    job, captured = _job(monkeypatch, table_options=TableOptions(generated_columns={"__generated_foo": "id + 1"}))

    job.create_table()

    assert captured["generated_columns"] == {"__generated_foo": "id + 1"}


def test_create_table_generated_columns_reject_non_prefixed_key(monkeypatch):
    job, _captured = _job(monkeypatch, table_options=TableOptions(generated_columns={"foo": "id + 1"}))

    with pytest.raises(AssertionError, match="__generated_"):
        job.create_table()


def test_create_table_primary_and_foreign_keys_passthrough(monkeypatch):
    from fabricks.models import ForeignKey, PrimaryKey

    job, captured = _job(
        monkeypatch,
        table_options=TableOptions(
            primary_key={"pk": PrimaryKey(keys=["id"])},
            foreign_keys={"fk": ForeignKey(keys=["name"], reference="gold.other")},
        ),
    )

    job.create_table()

    assert captured["primary_key"]["pk"].keys == ["id"]
    assert captured["foreign_keys"]["fk"].keys == ["name"]
    assert captured["foreign_keys"]["fk"].reference == "gold.other"
