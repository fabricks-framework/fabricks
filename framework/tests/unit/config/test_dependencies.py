"""Dependency-resolution decision logic, two layers:

- Gold/Silver `get_dependencies()`: parents/wait_for -> JobDependency list. Only the parents/wait_for
  path; the parser itself is covered by tests/unit/plain/test_dependency_parsing.py.
- `BaseStep._get_dependencies_internal()`: the (df, errors) aggregation around `run_in_parallel`,
  proven by monkeypatching the per-row worker `_get_dependencies` and `get_jobs()` (a plain list, with
  include_manual=True to skip the `df.where(...)` call a list does not support).
"""

import json
from pathlib import Path

from fabricks.core import get_job
from fabricks.core.jobs.silver import Silver
from fabricks.core.steps import get_step
import fabricks.core.steps.base as steps_base
from fabricks.models import JobDependency
from fabricks.utils.path.git import GitPath


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

    assert {(d.origin, d.parent) for d in deps} == {("parent", "gold.fact_other"), ("wait_for", "gold.fact_third")}


def test_gold_dependencies_wait_for_already_covered_by_parents_is_dropped():
    job = _gold_job(parents=["gold.fact_other"], wait_for=["GOLD.FACT_OTHER"])

    deps = job.get_dependencies()

    assert [(d.origin, d.parent) for d in deps] == [("parent", "gold.fact_other")]


def _silver_job(*, parents=None, wait_for=None):
    conf = {
        "step": "silver",
        "topic": "fact",
        "item": "dummy",
        "options": {"mode": "append", "parents": parents, "wait_for": wait_for},
    }
    return Silver(step="silver", topic="fact", item="dummy", conf=conf)


def test_silver_dependencies_parents_only():
    job = _silver_job(parents=["bronze.fact_other"])

    deps = job.get_dependencies()

    assert [(d.origin, d.parent) for d in deps] == [("parent", "bronze.fact_other")]


def test_silver_dependencies_default_to_the_parser_convention_without_parents():
    job = _silver_job(parents=None)

    deps = job.get_dependencies()

    assert [(d.origin, d.parent) for d in deps] == [("parser", "bronze.fact_dummy")]


def test_silver_dependencies_parents_and_wait_for():
    # unlike Gold, Silver has no "already covered by parents" guard on wait_for: every entry is appended
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


# The options.type == "manual" exclusion is a SQL predicate on a DataFrame, so it is tested against real Spark in
# tests/spark/apache/test_get_dependencies_manual.py.


def test_get_dependencies_internal_aggregates_dependencies_from_every_job(monkeypatch):
    step = get_step("gold")
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: [{"job": "a"}, {"job": "b"}, {"job": "c"}])
    by_job = {
        "a": [JobDependency.from_parts("a", "gold.x", "parent")],
        "b": [JobDependency.from_parts("b", "gold.x", "parent"), JobDependency.from_parts("b", "gold.y", "wait_for")],
        "c": [],
    }
    monkeypatch.setattr(
        steps_base, "_get_dependencies", lambda row: {"job": row["job"], "dependencies": by_job[row["job"]]}
    )
    captured: list[list[dict]] = []
    monkeypatch.setattr(steps_base.SPARK, "createDataFrame", lambda rows, _schema: captured.append(rows))

    _, errors = step._get_dependencies_internal(include_manual=True)

    assert errors == []
    assert sorted((r["job_id"], r["origin"], r["parent"]) for r in captured[0]) == [
        ("a", "parent", "gold.x"),
        ("b", "parent", "gold.x"),
        ("b", "wait_for", "gold.y"),
    ]


# Gold._get_notebook_dependencies() dispatch (static parse first, execution fallback): only the order of paths is
# tested here. The fallback is stubbed because it needs real Spark. Static parsing is opt-in
# (options.static_notebook_dependencies), so all but the first test turn `_use_static_notebook_parser` on.


_FALLBACK_SENTINEL = ["fallback.was_used"]
_FIXTURE_BASE = Path(__file__).parent.parent / "plain" / "fixtures" / "notebooks" / "dependency"


def _nb(*cell_sources: str) -> str:
    return json.dumps(
        {
            "cells": [{"cell_type": "code", "source": src} for src in cell_sources],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
    )


def _job_with_notebook_at(tmp_path, monkeypatch):
    job = get_job(step="gold", topic="fact", item="step_option")
    monkeypatch.setattr(job, "_notebook_path", lambda: GitPath(str(tmp_path / "nb")))
    monkeypatch.setattr(job, "_get_notebook_dependencies_by_execution", lambda: _FALLBACK_SENTINEL)
    monkeypatch.setattr(job, "_use_static_notebook_parser", lambda: True)
    return job


def test_execution_is_used_by_default_even_for_a_resolvable_ipynb(tmp_path, monkeypatch):
    # with static_notebook_dependencies unset, a parseable notebook must still take the execution fallback
    (tmp_path / "nb.ipynb").write_text(_nb('spark.sql("select * from gold.dim_time")'))
    job = get_job(step="gold", topic="fact", item="step_option")
    monkeypatch.setattr(job, "_notebook_path", lambda: GitPath(str(tmp_path / "nb")))
    monkeypatch.setattr(job, "_get_notebook_dependencies_by_execution", lambda: _FALLBACK_SENTINEL)

    assert job._get_notebook_dependencies() == _FALLBACK_SENTINEL


def test_uses_static_parse_result_when_only_ipynb_exists(tmp_path, monkeypatch):
    (tmp_path / "nb.ipynb").write_text(_nb('spark.sql("select * from gold.dim_time")'))
    job = _job_with_notebook_at(tmp_path, monkeypatch)

    assert job._get_notebook_dependencies() == ["gold.dim_time"]


def test_falls_back_when_static_parse_finds_nothing_resolvable(tmp_path, monkeypatch):
    (tmp_path / "nb.ipynb").write_text(_nb("df = some_frame.filter(some_frame.x > 1)"))
    job = _job_with_notebook_at(tmp_path, monkeypatch)

    assert job._get_notebook_dependencies() == _FALLBACK_SENTINEL


def test_falls_back_immediately_when_only_py_notebook_exists(tmp_path, monkeypatch):
    (tmp_path / "nb.py").write_text("pass")
    job = _job_with_notebook_at(tmp_path, monkeypatch)

    assert job._get_notebook_dependencies() == _FALLBACK_SENTINEL


def test_prefers_the_real_py_fixture_over_its_ipynb_sibling(monkeypatch):
    # dependency.py and dependency.ipynb coexist in fixtures/notebooks/ on purpose (see dependency.py's header)
    job = get_job(step="gold", topic="fact", item="step_option")
    monkeypatch.setattr(job, "_notebook_path", lambda: GitPath(str(_FIXTURE_BASE)))
    monkeypatch.setattr(job, "_get_notebook_dependencies_by_execution", lambda: _FALLBACK_SENTINEL)
    monkeypatch.setattr(job, "_use_static_notebook_parser", lambda: True)

    assert job._get_notebook_dependencies() == _FALLBACK_SENTINEL


def test_prefers_py_over_a_stale_ipynb_sibling(tmp_path, monkeypatch):
    # JobInvoker._run_notebook tries [None, ".py", ".ipynb"] in order, so a co-located ".ipynb" never runs and
    # must not be statically parsed
    (tmp_path / "nb.py").write_text("pass")
    (tmp_path / "nb.ipynb").write_text(_nb('spark.sql("select * from gold.should_be_ignored")'))
    job = _job_with_notebook_at(tmp_path, monkeypatch)

    assert job._get_notebook_dependencies() == _FALLBACK_SENTINEL
