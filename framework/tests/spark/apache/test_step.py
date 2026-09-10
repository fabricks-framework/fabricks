"""Step-level catalog bootstrap. create_db_objects() is left for a follow-up:
it calls job.create() for every job in the step, which for gold.fact.* needs
each job's own .sql file to exist (none of step_option/job_option/check have
one -- they only exist for config/option-resolution tests, never actually
run) -- needs a job with a real .sql body first.
"""

from fabricks.core.steps import get_step


def test_update_configurations_writes_gold_jobs(local_spark):
    step = get_step("gold")
    expected = step.get_jobs().count()

    step.update_configurations()

    assert local_spark.sql("select * from fabricks.gold_jobs").count() == expected


def test_update_dependencies_writes_silver_dependencies(local_spark):
    # Silver.get_dependencies() (unlike Gold's) never reads a .sql file --
    # explicit parents or a naming-convention fallback only -- so this is
    # safe against the DDL-only gold jobs' missing-.sql-file limitation above.
    step = get_step("silver")
    step.update_configurations()

    expected_df, errors = step._get_dependencies_internal()
    assert errors == []

    step.update_dependencies()

    assert local_spark.sql("select * from fabricks.silver_dependencies").count() == expected_df.count()
