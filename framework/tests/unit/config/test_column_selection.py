"""Generator._get_partitioning_columns/_get_clustering_columns
(framework/fabricks/core/jobs/base/generator.py:211-265): auto-detect
partition/cluster columns from a dataframe's column names and dtypes when
table_options doesn't declare them explicitly, otherwise use the explicit
list unconditionally. Pure functions of `df.columns`/`df.dtypes` and
`self.table_options` - no Spark execution happens inside either method, so
a plain stand-in object works in place of a real DataFrame.
"""

from fabricks.core import get_job
from fabricks.models.table import TableOptions
from tests.unit.config._helpers import _FakeDF


def _job():
    return get_job(step="gold", topic="fact", item="step_option")


def test_partitioning_columns_auto_detected_from_dunder_partition_prefix():
    df = _FakeDF(columns=["id", "__partition_date"])

    result = _job()._get_partitioning_columns(df)

    assert result == ["__partition_date"]


def test_partitioning_columns_none_when_nothing_matches():
    df = _FakeDF(columns=["id", "name"])

    result = _job()._get_partitioning_columns(df)

    assert result is None


def test_partitioning_columns_explicit_option_wins_over_auto_detect():
    job = _job()
    job.conf = job.conf.model_copy(update={"table_options": TableOptions(partition_by=["region"])})
    df = _FakeDF(columns=["id", "__partition_date"])

    result = job._get_partitioning_columns(df)

    assert result == ["region"]


def test_clustering_columns_auto_detected_from_known_dunder_columns():
    df = _FakeDF(columns=["id", "__key"], dtypes=[("id", "int"), ("__key", "string")])

    result = _job()._get_clustering_columns(df)

    assert result == ["__key"]


def test_clustering_columns_skips_boolean_typed_candidates():
    df = _FakeDF(columns=["id", "__is_current"], dtypes=[("id", "int"), ("__is_current", "boolean")])

    result = _job()._get_clustering_columns(df)

    assert result is None


def test_clustering_columns_explicit_option_wins_over_auto_detect():
    job = _job()
    job.conf = job.conf.model_copy(update={"table_options": TableOptions(cluster_by=["monarch"])})
    df = _FakeDF(columns=["id", "__key"], dtypes=[("id", "int"), ("__key", "string")])

    result = job._get_clustering_columns(df)

    assert result == ["monarch"]
