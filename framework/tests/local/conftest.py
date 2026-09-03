"""Conftest for local tests - real Spark+Delta, no Databricks. Applies 'local' marker.

IMPORTANT: the Delta-configured SparkSession below is built here, at this
file's *module import time* — not lazily inside a fixture body. pytest loads
a directory's conftest.py before collecting any test module in it, which is
what makes this win the race against fabricks.context's eager, non-Delta
SPARK singleton (see Task 7's "Why" note in the plan). Do not move this into
`local_spark`'s function body — that reintroduces the race.

Do not mix `tests/local` with `tests/unit`/`tests/databricks` in the same
pytest invocation — `fabricks.context.runtime` resolves CONF_RUNTIME once per
process, and the first import wins.
"""

import os
from pathlib import Path
import shutil

import pytest

# parents[2]: local -> tests -> framework. Deliberately NOT parents[3]/"framework"
# (the repo root then re-descending) - that only happens to work on the host.
# docker-compose.yml bind-mounts the "framework" dir itself to /workspace, so
# inside the container parents[3] lands on the container's filesystem root and
# FABRICKS_BASE would resolve to the nonexistent "/framework".
_FRAMEWORK_ROOT = Path(__file__).resolve().parents[2]
_LOCAL_STORAGE = _FRAMEWORK_ROOT / "tests" / "local" / ".storage"

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_CONFIG"] = "tests/local/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"

shutil.rmtree(_LOCAL_STORAGE, ignore_errors=True)

# get_spark()'s docker branch (enableHiveSupport(), no spark.sql.warehouse.dir
# override - Task 4, out of scope here) defaults Hive's embedded Derby
# metastore and warehouse to CWD (/workspace in the container, bind-mounted to
# this framework/ dir on the host - see docker-compose.yml). Without this,
# table registration from a previous `podman compose run` leaks into the next
# one (each `run --rm` is a fresh container, but /workspace is not).
for _leftover in ("metastore_db", "spark-warehouse"):
    shutil.rmtree(_FRAMEWORK_ROOT / _leftover, ignore_errors=True)
(_FRAMEWORK_ROOT / "derby.log").unlink(missing_ok=True)

from pyspark.sql import SparkSession  # noqa: E402

from fabricks.utils.spark import get_spark  # noqa: E402 - must follow the env-var setup above

_SPARK: SparkSession = get_spark()

# fabricks/metastore/table.py's Table._create() issues plain `create table
# ... location '...'` DDL with no `using delta` clause -- it relies entirely
# on the session's default table provider being Delta. Real Databricks
# Runtime configures that by default; get_spark()'s "docker" branch (Task 4)
# does not -- confirmed empirically (this container's real Spark session:
# `describe detail` on a table created via that exact DDL pattern showed
# `format=parquet`), so every table Table.create() makes here is silently a
# plain Hive/parquet table, and any later `merge into` on it raises
# `UnsupportedOperationException: ... does not support MERGE INTO TABLE`
# (reproduced exactly). Set here rather than in get_spark()'s docker branch
# (Task 4, not this plan's to touch) -- this is test-harness session setup,
# not a change to production table-creation DDL or CDC merge logic; it only
# makes the *unmodified* production DDL create the Delta tables it already
# assumes it's creating.
_SPARK.sql("set spark.sql.sources.default = delta")
# ponytail: fixtures are a handful of rows, but Spark's 200-partition shuffle
# default still fires on every merge - pure per-test overhead here, not real
# computation. Tuned for this session-scoped local-test JVM only; production
# get_spark() is untouched. (spark.ui.enabled is a static config - can't be
# changed post-getOrCreate(), skipping it here.)
_SPARK.conf.set("spark.sql.shuffle.partitions", "2")

from fabricks.metastore.database import Database  # noqa: E402 - must follow _SPARK's construction above

for _db_name in ("bronze", "silver", "gold", "expected"):
    Database(_db_name, spark=_SPARK).create()


def pytest_collection_modifyitems(items):
    """Automatically add 'local' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        try:
            if Path(item.fspath).is_relative_to(root):
                item.add_marker(pytest.mark.local)
        except (ValueError, AttributeError):
            if "local" in str(item.fspath):
                item.add_marker(pytest.mark.local)


@pytest.fixture(scope="session")
def local_spark():
    yield _SPARK
    _SPARK.stop()


@pytest.fixture(scope="session")
def king_and_queen_built(local_spark):
    """Factory fixture: returns a callable that advances one shared
    bronze/silver table pair through job N, one SCD1/SCD2.update() call per
    job (see Task 9's "Why job-sequential" note) — no job classes, no job
    config, no get_job().

    `_build(N)` applies job N exactly once, against whatever state job N-1
    left behind, against the *same* live table every call — not one fresh
    table per scenario. A caller must compare the result against expected
    output immediately, before requesting job N+1: since there's no
    snapshotting, a comparison made after the table has moved on to a later
    job would see that later job's state, not job N's (verified: reproduced
    exactly when this was briefly two independent loops over 1..9 instead of
    one). Requires strictly increasing N across the whole session (the one
    caller in test_silver.py loops 1..9 in order, comparing after each).

    Outer fixture is session-scoped (creates the `expected` views once,
    idempotently, and owns the one running SCD1/SCD2 pair); the returned
    `_build` callable is invoked per job number.
    See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #5.
    """
    from fabricks.cdc.nocdc import NoCDC
    from fabricks.cdc.scd1 import SCD1
    from fabricks.cdc.scd2 import SCD2

    from tests.local.compare import create_expected_views

    create_expected_views(local_spark, "silver", "scd2")
    create_expected_views(local_spark, "silver", "scd1")

    scd2 = SCD2("silver", "king_and_queen", "scd2", spark=local_spark)
    scd1 = SCD1("silver", "king_and_queen", "scd1", spark=local_spark)
    _last_applied = 0

    def _build(through_job: int) -> dict:
        nonlocal _last_applied
        assert through_job == _last_applied + 1, (
            f"king_and_queen_built must be called in order: expected job {_last_applied + 1}, got {through_job}"
        )

        for job_num in [through_job]:
            fixtures_root = Path(__file__).resolve().parent / "fixtures" / f"job{job_num}"

            job_dfs = []
            for entity in ("king", "queen"):
                # spark.read.json(path), not createDataFrame(list-of-dicts):
                # every job's fixture carries BEL_DeleteDateUtc/
                # BEL_RestoredDateUtc columns that are null in every single
                # row (no deletes/restores ever happen in this fixture data).
                # createDataFrame's list-of-dicts schema inference raises
                # PySparkValueError(CANNOT_DETERMINE_TYPE) for an
                # all-null column (verified empirically against this
                # container's real Spark session) -- Spark's JSON-file reader
                # uses a different, more lenient inference path that types an
                # all-null column as nullable StringType instead of failing.
                path = str(fixtures_root / f"bronze_{entity}_scd1.jsonl")
                df = local_spark.read.json(path)
                # append, not overwrite: bronze accumulates across jobs,
                # matching production's landing-table semantics. The SILVER
                # merge below uses this job's own `df` directly, not
                # nocdc.table.dataframe (which would be every job's rows
                # accumulated so far) — each job's .update() call gets only
                # that job's new rows; the CDC merge itself is what compares
                # against the already-merged silver state from prior jobs.
                nocdc = NoCDC("bronze", f"{entity}_king_and_queen", "scd1", spark=local_spark)
                nocdc.append(df)
                job_dfs.append(df)

            combined = job_dfs[0].unionByName(job_dfs[1], allowMissingColumns=True)

            # One .update() call per job, sequentially: job N's call runs
            # against whatever table state job N-1's call left behind.
            #
            # add_key=True, keys="id" (a string, not a list) -- both needed,
            # for two separate reasons, discovered by reproducing the exact
            # malformed generated MERGE SQL against this container's real
            # Spark session:
            #
            # 1. add_key=True: without it, Processor.get_query_context()'s
            # `has_key` (computed from the add_key kwarg BEFORE mode="update"
            # forces add_key on internally, so the early, un-forced value is
            # what sticks) stays False, so `__key` never makes it into the
            # merged query view's own output columns. merge.sql.jinja's
            # merge-condition branches on exactly that column's presence: with
            # __key it emits the simple `on t.__key == s.__merge_key`; without
            # it, it falls back to a per-`keys`-entry equality loop whose
            # jinja source (fabricks/cdc/templates/merges/scd2.sql.jinja)
            # unconditionally appends a trailing " and" after *every* key
            # (including the last) and then unconditionally emits a further
            # "and t.__is_current" line right after -- so that branch always
            # renders a bare "and\n  and", a syntax error, regardless of how
            # many keys there are. fabricks/core/jobs/silver.py's
            # get_cdc_context() always sets context["add_key"] = True for
            # slowly-changing-dimension jobs (see its line ~314) -- this
            # mirrors that, and takes the same __key-based branch real Silver
            # jobs do, rather than the effectively-dead __else__ branch this
            # test would otherwise be the first caller to ever exercise.
            #
            # 2. keys="id", not keys=["id"]: independently of (1), a caller-
            # supplied *list* is also unsafe here because Merger.merge() calls
            # get_query_context() twice within one .update() call whenever the
            # target table doesn't exist yet (once via create_table()'s own
            # get_data() call, once via merge()'s subsequent get_data() call)
            # -- both sharing the same kwargs dict, and therefore the same
            # "keys" list object (passed by reference through nested **kwargs
            # unpacking). get_query_context()'s `keys.append("__source")`
            # mutates that object *in place*, so the second invocation appends
            # "__source" again. A plain string sidesteps this: both
            # get_query_context() and get_merge_context() already special-case
            # `isinstance(keys, str)` by rebinding to a *new* one-element list
            # (`keys = [keys]`) rather than mutating anything, so the
            # immutable string kwargs entry is untouched across repeat
            # invocations.
            #
            # Both are latent bugs in fabricks/cdc/base/processor.py's `keys`/
            # `add_key` handling, not bugs in this test -- but production never
            # trips either one, since get_cdc_context() always sets add_key
            # explicitly and never passes an explicit "keys" kwarg at all
            # (letting it default to a freshly-built list(fields) every call).
            # Out of this test's scope to fix in fabricks/cdc/ itself; using
            # the calling convention real Silver jobs already use avoids both
            # without touching production code or changing which code path
            # this test exercises.
            #
            # soft_delete=True: fabricks/core/jobs/silver.py's
            # get_cdc_context() always sets context["soft_delete"] =
            # self.slowly_changing_dimension (True for scd1/scd2). Without
            # it, Processor.get_query_context()'s `outputs.append` for
            # __is_deleted (both cdc types) and __is_current (scd1 only --
            # scd2 always adds __is_current unconditionally) never fires, so
            # those columns are silently missing from the produced table
            # (verified: UNRESOLVED_COLUMN against this container's real
            # Spark session when the expected side has them and the actual
            # table doesn't).
            #
            # correct_valid_from=True (scd2 only): fabricks/core/jobs/
            # silver.py's get_cdc_context() always sets
            # context["correct_valid_from"] = True when change_data_capture
            # == "scd2". It's what rewrites __valid_from to the 1900-01-01
            # sentinel for every row sharing the batch's global-minimum
            # __timestamp (scd2.sql.jinja's `__correct_valid_from` CTE:
            # `min(__valid_from) over (partition by null)` -- a true global
            # window, no partitioning, so *every* row tied for the minimum
            # gets rewritten, not just one). Without it, a first-ever load's
            # oldest row(s) keep their real batch timestamp instead of the
            # epoch-start sentinel the expected oracle encodes (verified:
            # job1's king batch-1 (id=1, id=2) and queen batch-1 (id=101)
            # all share the identical batch-derived timestamp
            # "2022-01-01T00:01:00", so all three legitimately become
            # 1900-01-01 -- not a one-row special case).
            #
            # update_schema() before update(), when the table already exists
            # (skipped on each CDC object's first-ever call, where
            # create_table() already builds the schema fresh from that job's
            # own data): real Silver jobs widen the target table's schema
            # between incremental runs (this plan deliberately has no such
            # orchestration step -- see the "no update_schema() between
            # jobs" note in fabricks/utils/spark.py's docker branch).
            # Without it, job2's newField column (present in its own source
            # rows but absent from job1-created table) breaks the merge
            # query's own rectify CTE (fabricks/cdc/templates/ctes/
            # rectify.sql.jinja's __rectified_base, which selects
            # `intermediates` -- including newField once any job introduces
            # it -- by name from *both* the incoming batch and `__current`,
            # a CTE reading the existing target table's own rows): verified
            # UNRESOLVED_COLUMN against this container's real Spark session,
            # listing only the target table's pre-existing 5 columns.
            # autoMerge (spark.databricks.delta.schema.autoMerge.enabled)
            # only patches the final `merge into` statement -- it doesn't
            # retroactively widen an arbitrary SELECT reading the
            # not-yet-altered target table mid-query. update_schema() (an
            # existing, already-public Generator method -- not new
            # production code) issues the real ALTER TABLE ADD COLUMNS
            # ahead of time, exactly what real job orchestration does
            # between runs.
            if scd2.table.exists():
                scd2.update_schema(combined, keys="id", add_key=True, soft_delete=True, correct_valid_from=True)
            if scd1.table.exists():
                scd1.update_schema(combined, keys="id", add_key=True, soft_delete=True)

            scd2.update(combined, keys="id", add_key=True, soft_delete=True, correct_valid_from=True)
            scd1.update(combined, keys="id", add_key=True, soft_delete=True)

        _last_applied = through_job
        return {"scd1": scd1, "scd2": scd2}

    return _build
