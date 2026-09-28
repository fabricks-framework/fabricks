"""Gold._get_notebook_dependencies() (framework/fabricks/core/jobs/gold.py): the
static-parse-first/execution-fallback dispatch. The static parser itself (ast/sqlglot
extraction) is pure Python and covered by tests/unit/plain/test_notebook_dependencies.py;
this file covers only the wiring decision -- which path gets tried, and in what order --
by pointing a real Gold job's `_notebook_path()` at a tmp_path notebook and stubbing
`_get_notebook_dependencies_by_execution()` so the (real-Spark-only) execution fallback is
never actually invoked, only observed.

Static parsing is opt-in (runtime.yml's options.static_notebook_dependencies, off by
default -- see RuntimeOptions in fabricks/models/runtime/models.py), so every test here
except test_execution_is_used_by_default_even_for_a_resolvable_ipynb monkeypatches
`_use_static_notebook_parser` on to force the opted-in path.
"""

import json
from pathlib import Path

from fabricks.core import get_job
from fabricks.utils.path.git import GitPath

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
