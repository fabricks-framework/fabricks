"""Reproduces https://github.com/fabricks-framework/fabricks/issues/183:
see the ordered-retry comment on BaseStep._create_db_objects_internal()
(framework/fabricks/core/steps/base.py) for the mechanism.

Uses the same monkeypatch pattern as test_dependencies.py's
test_get_dependencies_internal_* tests: `get_jobs` is monkeypatched to a
MagicMock DataFrame (`_create_db_objects_internal` calls `.where(...)` and
`.select(...).collect()` on it), and the module-level `run_in_parallel` is
monkeypatched to a fake with per-call behavior, sidestepping the real
get_job_internal()/YAML-lookup/Spark-SQL chain.
"""

from unittest.mock import MagicMock

from fabricks.core.steps import get_step
import fabricks.core.steps.base as steps_base


def _jobs_df(rows):
    df = MagicMock()
    df.select.return_value.collect.return_value = rows
    df.cache.return_value = df  # real DataFrame.cache() returns self
    return df


def _missing_view(name: str) -> Exception:
    return Exception(f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `gold`.`{name}` cannot be found.")


def test_create_db_objects_internal_resolves_a_depth_2_dependency_chain(monkeypatch):
    # A -> B -> C: pass 1 registers C but not A/B; A's blocker (B) is
    # itself still failing so A waits, but B's blocker (C) already
    # succeeded, so pass 2 retries B; pass 3 retries A now that B exists.
    step = get_step("gold")
    monkeypatch.setattr(
        step,
        "get_jobs",
        lambda topic=None: _jobs_df(
            [
                {"topic": "qlikview", "item": "artikel", "job_id": "A"},
                {"topic": "qlikview", "item": "leverancier", "job_id": "B"},
                {"topic": "qlikview", "item": "po_lines", "job_id": "C"},
            ]
        ),
    )

    workers_per_call: list = []

    def _fake(_fn, _df, **kwargs):
        workers_per_call.append(kwargs.get("workers"))
        n = len(workers_per_call)
        if n == 1:
            return [
                {"job": "A", "job_id": "A", "error": _missing_view("qlikview_leverancier")},
                {"job": "B", "job_id": "B", "error": _missing_view("qlikview_po_lines")},
                {"job": "C", "job_id": "C"},
            ]
        if n == 2:
            return [{"job": "B", "job_id": "B"}]
        return [{"job": "A", "job_id": "A"}]

    monkeypatch.setattr(steps_base, "run_in_parallel", _fake)

    _, errors = step._create_db_objects_internal(update_lists=False, max_attempts=3)

    assert errors == [], f"expected the chain to fully resolve within 3 attempts, got: {errors}"
    assert workers_per_call == [16, 16, 16], "each pass (incl. retries) still dispatches its ready set in parallel"


def test_create_db_objects_internal_stops_retrying_on_a_genuine_error(monkeypatch):
    # A non-race error (e.g. a missing column) never disappears between
    # passes -- retrying it is wasted work, and the loop must stop as soon
    # as the failing set stops shrinking rather than burning every attempt.
    step = get_step("gold")
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: _jobs_df([{"topic": "t", "item": "a", "job_id": "A"}]))

    calls: list = []

    def _fake(_fn, _df, **_kwargs):
        calls.append(1)
        return [{"job": "A", "job_id": "A", "error": Exception("column `x` does not exist")}]

    monkeypatch.setattr(steps_base, "run_in_parallel", _fake)

    _, errors = step._create_db_objects_internal(update_lists=False, max_attempts=5)

    assert len(errors) == 1
    assert len(calls) == 2, "should stop after the first retry shows no progress, not exhaust all 5 attempts"


def test_create_db_objects_internal_first_pass_can_run_sequentially(monkeypatch):
    step = get_step("gold")
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: _jobs_df([{"topic": "t", "item": "a", "job_id": "A"}]))

    workers_per_call: list = []
    monkeypatch.setattr(
        steps_base,
        "run_in_parallel",
        lambda _fn, _df, **kwargs: workers_per_call.append(kwargs.get("workers")) or [{"job": "A", "job_id": "A"}],
    )

    step._create_db_objects_internal(update_lists=False, parallel=False)

    assert workers_per_call == [1]
