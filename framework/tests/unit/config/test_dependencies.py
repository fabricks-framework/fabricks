"""Dependency-resolution decision logic, two layers:

- Gold/Silver `get_dependencies()`: parents/wait_for -> JobDependency list. Only the parents/wait_for
  path; the notebook branch needs real Spark (see test_notebook_dependencies_wiring.py) and the
  SQL-parsed branch is covered by tests/unit/plain/test_sql_dependencies.py.
- `BaseStep._get_dependencies_internal()`: the (df, errors) aggregation around `run_in_parallel`,
  proven by monkeypatching the per-row worker `_get_dependencies` and `get_jobs()` (a plain list, with
  include_manual=True to skip the `df.where(...)` call a list does not support).
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

from fabricks.core import get_job
from fabricks.core.jobs.silver import Silver
from fabricks.core.steps import get_step
import fabricks.core.steps.base as steps_base
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


# options.type == "manual" (fabricks/models/common.py's AllowedTypes) marks a
# job as excluded from automatic scheduling/dependency resolution -- it must
# be run out of band. _get_dependencies_internal() is where that exclusion is
# enforced, via a `df.where(...)` call get_jobs() must return a real
# DataFrame for -- a plain list (as used above) doesn't support `.where`, so
# this needs a DataFrame-shaped mock instead.
def test_get_dependencies_internal_excludes_manual_jobs_by_default(monkeypatch):
    step = get_step("gold")
    mock_jobs_df = MagicMock()
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: mock_jobs_df)
    monkeypatch.setattr(steps_base, "run_in_parallel", lambda *args, **kwargs: [])

    step._get_dependencies_internal()

    mock_jobs_df.where.assert_called_once_with("not options.type <=> 'manual'")


def test_get_dependencies_internal_keeps_manual_jobs_when_requested(monkeypatch):
    step = get_step("gold")
    mock_jobs_df = MagicMock()
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: mock_jobs_df)
    monkeypatch.setattr(steps_base, "run_in_parallel", lambda *args, **kwargs: [])

    step._get_dependencies_internal(include_manual=True)

    mock_jobs_df.where.assert_not_called()


# --- Gold._get_notebook_dependencies(): the static-parse-first / execution-fallback dispatch. The parser itself is
# --- covered by tests/unit/plain/test_dependency_parsing.py; this covers only which path is tried and in what order,
# --- by pointing a real Gold job's `_notebook_path()` at a tmp_path notebook and stubbing
# --- `_get_notebook_dependencies_by_execution()` so the real-Spark-only fallback is observed, never run.
# --- Static parsing is opt-in (runtime.yml's options.static_notebook_dependencies, off by default), so every test
# --- here except the first monkeypatches `_use_static_notebook_parser` on.


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
    # static_notebook_dependencies is opt-in -- with it unset, a perfectly parseable notebook
    # must still go through the execution fallback, not the static parser.
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
    # dependency.py and dependency.ipynb coexist in fixtures/notebooks/ on purpose (see
    # dependency.py's header comment) -- exercises the precedence rule against real fixture
    # files rather than synthetic tmp_path stand-ins.
    job = get_job(step="gold", topic="fact", item="step_option")
    monkeypatch.setattr(job, "_notebook_path", lambda: GitPath(str(_FIXTURE_BASE)))
    monkeypatch.setattr(job, "_get_notebook_dependencies_by_execution", lambda: _FALLBACK_SENTINEL)
    monkeypatch.setattr(job, "_use_static_notebook_parser", lambda: True)

    assert job._get_notebook_dependencies() == _FALLBACK_SENTINEL


def test_prefers_py_over_a_stale_ipynb_sibling(tmp_path, monkeypatch):
    # invoker._run_notebook tries [None, ".py", ".ipynb"] in that order -- if a ".py" file
    # exists, that's what actually runs, so dependency resolution must not statically parse a
    # co-located ".ipynb" that invoker would never touch.
    (tmp_path / "nb.py").write_text("pass")
    (tmp_path / "nb.ipynb").write_text(_nb('spark.sql("select * from gold.should_be_ignored")'))
    job = _job_with_notebook_at(tmp_path, monkeypatch)

    assert job._get_notebook_dependencies() == _FALLBACK_SENTINEL
