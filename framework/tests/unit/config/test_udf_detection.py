"""Gold.get_udfs(): UDF detection from updater_options.columns and from the job's own SQL (the updater_options
path is the one a refactor once dropped). mode="invoke" isolates the updater_options path from SQL matching; the SQL
tests monkeypatch get_sql() instead of relying on a runtime .sql fixture."""

from fabricks.core import get_job
from fabricks.models.common import UpdaterOptions


def _job_with_updater_columns(**columns: str):
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(
        update={
            "options": job.conf.options.model_copy(update={"mode": "invoke"}),
            "updater_options": UpdaterOptions(columns=columns),
        }
    )
    return job


def test_get_udfs_detects_udf_call_in_updater_options_column():
    job = _job_with_updater_columns(__updated_extra_column="udf_add_now(monarch)")

    assert job.get_udfs() == ["add_now"]


def test_get_udfs_detects_multiple_udfs_across_updater_options_columns():
    job = _job_with_updater_columns(
        __updated_extra_column="udf_add_now(monarch)", __updated_other_column="udf_slugify(name)"
    )

    assert sorted(job.get_udfs() or []) == ["add_now", "slugify"]


def test_get_udfs_ignores_non_udf_updater_options_column():
    job = _job_with_updater_columns(__updated_new_column="(1+1)")

    assert job.get_udfs() == []


def test_get_udfs_none_when_no_updater_options_set():
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(update={"options": job.conf.options.model_copy(update={"mode": "invoke"})})

    assert job.get_udfs() is None


def test_get_udfs_detects_udf_call_in_job_sql(monkeypatch):
    job = get_job(step="gold", topic="fact", item="step_option")
    monkeypatch.setattr(job, "get_sql", lambda: "select udf_add_now(monarch) as ts from silver.monarch")

    assert job.get_udfs() == ["add_now"]


def test_get_udfs_detects_multiple_udfs_in_job_sql(monkeypatch):
    job = get_job(step="gold", topic="fact", item="step_option")
    monkeypatch.setattr(job, "get_sql", lambda: "select udf_add_now(monarch), udf_slugify(name) from silver.monarch")

    assert sorted(job.get_udfs() or []) == ["add_now", "slugify"]


def test_get_udfs_none_when_job_sql_has_no_udf_call(monkeypatch):
    job = get_job(step="gold", topic="fact", item="step_option")
    monkeypatch.setattr(job, "get_sql", lambda: "select monarch, name from silver.monarch")

    assert job.get_udfs() is None


def test_get_udfs_merges_updater_options_and_sql_udfs(monkeypatch):
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(
        update={"updater_options": UpdaterOptions(columns={"__updated_extra_column": "udf_add_now(monarch)"})}
    )
    monkeypatch.setattr(job, "get_sql", lambda: "select udf_slugify(name) from silver.monarch")

    assert sorted(job.get_udfs() or []) == ["add_now", "slugify"]


def test_get_udfs_lists_a_udf_once_even_when_it_is_called_several_times(monkeypatch):
    job = get_job(step="gold", topic="fact", item="step_option")
    monkeypatch.setattr(job, "get_sql", lambda: "select udf_add_now(a), udf_add_now(b) from silver.monarch")

    assert job.get_udfs() == ["add_now"]


def test_get_udfs_lists_a_udf_once_when_both_updater_options_and_sql_call_it(monkeypatch):
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(
        update={"updater_options": UpdaterOptions(columns={"__updated_extra_column": "udf_add_now(monarch)"})}
    )
    monkeypatch.setattr(job, "get_sql", lambda: "select udf_add_now(name) from silver.monarch")

    assert job.get_udfs() == ["add_now"]


def test_get_udfs_ignores_job_sql_for_a_table_job():
    # udfs are not allowed in a job that reads a table: only the updater_options columns count
    job = get_job(step="gold", topic="fact", item="table_option")
    job.conf = job.conf.model_copy(
        update={"updater_options": UpdaterOptions(columns={"__updated_extra_column": "udf_add_now(monarch)"})}
    )

    assert job.get_udfs() == ["add_now"]
