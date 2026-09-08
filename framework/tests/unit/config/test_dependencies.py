"""Dependency-resolution decision logic, three layers:

- Gold.get_dependencies() (framework/fabricks/core/jobs/gold.py:183-216) and
  Silver.get_dependencies() (framework/fabricks/core/jobs/silver.py:172-189):
  parents/wait_for -> JobDependency list. Stays on the parents/wait_for path
  only - the notebook-dependency branch needs a real
  `spark.sql("explain extended...")` (out of scope for this tier) and the
  SQL-parsed branch (no parents/wait_for at all) is already covered
  pure-Python by tests/unit/plain/test_sql_dependencies.py.

- BaseStep._get_dependencies_internal() (framework/fabricks/core/steps/
  base.py:162-207): the (df, errors) aggregation around run_in_parallel -
  proven by monkeypatching the per-row worker (`_get_dependencies`, a
  module-level function in fabricks.core.steps.base) directly, sidestepping
  the full get_job_internal()/YAML-lookup chain (Gold/Bronze.from_job_id
  don't forward an injected `conf`, so a synthetic "bad" job_id can't be
  fed through it without either a real bad YAML entry or raising before the
  per-row try/except even runs). get_jobs() is monkeypatched to a plain
  list (not a DataFrame) - include_manual=True sidesteps the `df.where(...)`
  call _get_dependencies_internal makes on non-manual jobs, which a plain
  list doesn't support.
"""

import fabricks.core.steps.base as steps_base
from fabricks.core import get_job
from fabricks.core.jobs.silver import Silver
from fabricks.core.steps import get_step


def _gold_job(*, parents=None, wait_for=None):
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(
        update={"options": job.conf.options.model_copy(update={"parents": parents, "wait_for": wait_for})}
    )
    return job


def test_gold_dependencies_parents_only():
    job = _gold_job(parents=["gold.fact_other"])

    deps = job.get_dependencies()

    assert [(d.origin, d.parent) for d in deps] == [("parent", "gold.fact_other")]


def test_gold_dependencies_parents_and_new_wait_for():
    job = _gold_job(parents=["gold.fact_other"], wait_for=["gold.fact_third"])

    deps = job.get_dependencies()

    assert {(d.origin, d.parent) for d in deps} == {
        ("parent", "gold.fact_other"),
        ("wait_for", "gold.fact_third"),
    }


def test_gold_dependencies_wait_for_already_covered_by_parents_is_dropped():
    job = _gold_job(parents=["gold.fact_other"], wait_for=["GOLD.FACT_OTHER"])

    deps = job.get_dependencies()

    assert [(d.origin, d.parent) for d in deps] == [("parent", "gold.fact_other")]


def _silver_job(*, parents=None, wait_for=None):
    conf = {
        "step": "silver_test",
        "topic": "fact",
        "item": "dummy",
        "options": {"mode": "append", "parents": parents, "wait_for": wait_for},
    }
    # Explicit conf (row=) makes get_job_conf bypass YAML/STEPS entirely, so
    # "silver_test" need not be a registered step - fine here since parents
    # is always non-empty, so get_dependencies() never touches
    # self.parent_step (which would need a real step_conf).
    return Silver(step="silver_test", topic="fact", item="dummy", conf=conf)


def test_silver_dependencies_parents_only():
    job = _silver_job(parents=["bronze.fact_other"])

    deps = job.get_dependencies()

    assert [(d.origin, d.parent) for d in deps] == [("parent", "bronze.fact_other")]


def test_silver_dependencies_parents_and_wait_for():
    # unlike Gold, Silver.get_dependencies() has no "already covered by
    # parents" guard on wait_for - every entry is appended unconditionally.
    job = _silver_job(parents=["bronze.fact_other"], wait_for=["bronze.fact_other", "bronze.fact_third"])

    deps = job.get_dependencies()

    assert [(d.origin, d.parent) for d in deps] == [
        ("parent", "bronze.fact_other"),
        ("wait_for", "bronze.fact_other"),
        ("wait_for", "bronze.fact_third"),
    ]


def test_get_dependencies_internal_collects_errors_alongside_successes(monkeypatch):
    step = get_step("gold")
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: [{"job": "good"}, {"job": "bad"}])

    def _fake_get_dependencies(row):
        if row["job"] == "bad":
            return {"job": "bad", "error": ValueError("boom")}
        return {"job": "good", "dependencies": []}

    monkeypatch.setattr(steps_base, "_get_dependencies", _fake_get_dependencies)

    _, errors = step._get_dependencies_internal(include_manual=True)

    assert len(errors) == 1
    assert errors[0]["job"] == "bad"
    assert isinstance(errors[0]["error"], ValueError)


def test_get_dependencies_internal_no_errors_when_all_succeed(monkeypatch):
    step = get_step("gold")
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: [{"job": "good1"}, {"job": "good2"}])
    monkeypatch.setattr(steps_base, "_get_dependencies", lambda row: {"job": row["job"], "dependencies": []})

    _, errors = step._get_dependencies_internal(include_manual=True)

    assert errors == []
