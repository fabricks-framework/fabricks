"""Reproduces https://github.com/fabricks-framework/fabricks/issues/183:
see the ordered-retry comment on BaseStep._create_db_objects_internal()
(framework/fabricks/core/steps/base.py) for the mechanism.

`get_jobs` returns a `_JobsDF` that really filters on `.where(df["job_id"].isin(...))`, and the
module-level `run_in_parallel` is replaced by a fake that records which job ids each pass was handed, so
the tests assert the whole dispatch sequence, not just the final errors.
"""

from fabricks.core.steps import get_step
import fabricks.core.steps.base as steps_base


class _Column:
    def __init__(self, name: str) -> None:
        self.name = name

    def isin(self, values: list[str]) -> tuple[str, frozenset[str]]:
        return (self.name, frozenset(values))


class _JobsDF:
    """The slice of DataFrame that `_create_db_objects_internal` touches, with a real `where`."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def __getitem__(self, name: str) -> _Column:
        return _Column(name)

    def cache(self) -> "_JobsDF":
        return self

    def unpersist(self) -> None:
        pass

    def select(self, *columns: str) -> "_JobsDF":
        return _JobsDF([{c: r[c] for c in columns} for r in self.rows])

    def collect(self) -> list[dict]:
        return self.rows

    def where(self, predicate: tuple[str, frozenset[str]]) -> "_JobsDF":
        column, values = predicate
        return _JobsDF([r for r in self.rows if r[column] in values])


def _jobs(*job_ids: str) -> _JobsDF:
    return _JobsDF([{"topic": "t", "item": job_id.lower(), "job_id": job_id} for job_id in job_ids])


def _missing_view(item: str) -> Exception:
    return Exception(f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `gold`.`t_{item}` cannot be found.")


def _failed(job_id: str, error: Exception) -> dict:
    return {"job": job_id, "job_id": job_id, "error": error}


def _ok(job_id: str) -> dict:
    return {"job": job_id, "job_id": job_id}


class _Dispatch:
    """Scripted run_in_parallel: pass N returns `passes[N]`, and every call records its job ids and workers."""

    def __init__(self, monkeypatch, passes: list[list[dict]]) -> None:
        self.passes = passes
        self.job_ids: list[set[str]] = []
        self.workers: list[int] = []
        monkeypatch.setattr(steps_base, "run_in_parallel", self)

    def __call__(self, _fn, df: _JobsDF, *, workers: int, **_kwargs) -> list[dict]:
        self.job_ids.append({r["job_id"] for r in df.rows})
        self.workers.append(workers)
        return self.passes[len(self.job_ids) - 1]


def _step(monkeypatch, jobs: _JobsDF):
    step = get_step("gold")
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: jobs)
    return step


# A -> B -> C: pass 1 registers C but not A/B. A's blocker (B) is itself still failing so A waits, but B's
# blocker (C) already succeeded, so pass 2 retries only B; pass 3 retries only A now that B exists.
_CHAIN_PASS_1 = [_failed("A", _missing_view("b")), _failed("B", _missing_view("c")), _ok("C")]


def test_create_db_objects_internal_resolves_a_depth_2_dependency_chain(monkeypatch):
    step = _step(monkeypatch, _jobs("A", "B", "C"))
    dispatch = _Dispatch(monkeypatch, [_CHAIN_PASS_1, [_ok("B")], [_ok("A")]])

    _, errors = step._create_db_objects_internal(update_lists=False, max_attempts=3)

    assert errors == []
    assert dispatch.job_ids == [{"A", "B", "C"}, {"B"}, {"A"}], "each retry must dispatch only the unblocked failures"
    assert dispatch.workers == [16, 16, 16]


def test_create_db_objects_internal_stops_at_max_attempts(monkeypatch):
    step = _step(monkeypatch, _jobs("A", "B", "C"))
    dispatch = _Dispatch(monkeypatch, [_CHAIN_PASS_1, [_ok("B")]])

    _, errors = step._create_db_objects_internal(update_lists=False, max_attempts=2)

    assert dispatch.job_ids == [{"A", "B", "C"}, {"B"}]
    assert [e["job_id"] for e in errors] == ["A"], "A was still blocked when the attempts ran out"


def test_create_db_objects_internal_stops_when_every_failure_is_blocked_on_another(monkeypatch):
    step = _step(monkeypatch, _jobs("A", "B"))
    dispatch = _Dispatch(monkeypatch, [[_failed("A", _missing_view("b")), _failed("B", _missing_view("a"))]])

    _, errors = step._create_db_objects_internal(update_lists=False, max_attempts=5)

    assert dispatch.job_ids == [{"A", "B"}], "a circular block can never progress; retrying it is wasted work"
    assert sorted(e["job_id"] for e in errors) == ["A", "B"]


def test_create_db_objects_internal_does_not_retry_when_retry_is_false(monkeypatch):
    step = _step(monkeypatch, _jobs("A", "B"))
    dispatch = _Dispatch(monkeypatch, [[_failed("A", _missing_view("b")), _ok("B")]])

    _, errors = step._create_db_objects_internal(update_lists=False, retry=False)

    assert dispatch.job_ids == [{"A", "B"}]
    assert [e["job_id"] for e in errors] == ["A"]


def test_create_db_objects_internal_stops_retrying_on_a_genuine_error(monkeypatch):
    # A non-race error (e.g. a missing column) never disappears between passes: retrying it is wasted work,
    # and the loop must stop as soon as the failing set stops shrinking.
    step = _step(monkeypatch, _jobs("A"))
    dispatch = _Dispatch(monkeypatch, [[_failed("A", Exception("column `x` does not exist"))]] * 5)

    _, errors = step._create_db_objects_internal(update_lists=False, max_attempts=5)

    assert [e["job_id"] for e in errors] == ["A"]
    assert dispatch.job_ids == [{"A"}, {"A"}], "should stop after the first retry shows no progress, not use all 5"


def test_create_db_objects_internal_first_pass_can_run_sequentially(monkeypatch):
    step = _step(monkeypatch, _jobs("A"))
    dispatch = _Dispatch(monkeypatch, [[_ok("A")]])

    step._create_db_objects_internal(update_lists=False, parallel=False)

    assert dispatch.workers == [1]
