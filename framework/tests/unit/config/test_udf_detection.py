"""Gold.get_udfs() (framework/fabricks/core/jobs/gold.py): UDF detection
from updater_options.columns and from the job's own SQL. Regression test
for a real bug: the updater_options.columns path was dropped during the
job-composition refactor on the (wrong) assumption that no job's updated
columns ever call a UDF -- fabricks.legacy's gold.scd1.updated_column job
does (`udf_add_now(monarch)`), so the UDF never got registered before the
update expression ran. mode="invoke" is used for the updater_options-only
tests to isolate that path from SQL-based matching; the SQL tests below
monkeypatch get_sql() instead of relying on a real runtime .sql fixture.
"""

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

    assert set(job.get_udfs() or []) == {"add_now", "slugify"}


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

    assert set(job.get_udfs() or []) == {"add_now", "slugify"}


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

    assert set(job.get_udfs() or []) == {"add_now", "slugify"}
