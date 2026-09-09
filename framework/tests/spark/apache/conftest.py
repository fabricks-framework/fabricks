"""Conftest for tests/spark/apache - real Spark+Delta, no Databricks Runtime
("apache" = Apache Spark, as opposed to a vendor runtime).
Applies the 'apache' marker.

IMPORTANT: the Delta-configured SparkSession below is built here, at this
file's *module import time* — not lazily inside a fixture body. pytest loads
a directory's conftest.py before collecting any test module in it, which is
what makes this win the race against fabricks.context's eager, non-Delta
SPARK singleton (see Task 7's "Why" note in the plan). Do not move this into
`local_spark`'s function body — that reintroduces the race.

Do not mix `tests/spark/apache` with `tests/unit/plain`/`tests/spark/databricks`/
`tests/unit/config` in the same pytest invocation — `fabricks.context.runtime`
resolves CONF_RUNTIME once per process, and the first import wins.
"""

import os
from pathlib import Path
import shutil
import time

import pytest

# parents[3]: apache -> spark -> tests -> framework.
_FRAMEWORK_ROOT = Path(__file__).resolve().parents[3]

# Under pytest-xdist, each worker is a separate process importing this module
# independently -- keyed off PYTEST_XDIST_WORKER (unset -> "gw0" for a plain,
# non-xdist run).
_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "gw0")

# Each layer's storage is a single fixed absolute path -- not worker-scoped,
# unlike the Derby/warehouse dirs below. Every worker wiping the same shared
# tree at import time is safe: pytest-xdist has every worker finish its own
# collection (which is what triggers this import) before the controller
# dispatches any test item to any of them, so this always happens before any
# worker's tests start writing table data -- there's no window where one
# worker's rmtree could delete another's in-progress output.
_LOCAL_STORAGE = _FRAMEWORK_ROOT / "tests" / "spark" / "apache" / ".storage"
_EXPECTED_CACHE = _FRAMEWORK_ROOT / "tests" / "spark" / "apache" / ".expected-cache"
_WORKER_ROOT = _FRAMEWORK_ROOT / "tests" / "spark" / "apache" / ".worker_cwd" / _WORKER
_WORKER_ROOT.mkdir(parents=True, exist_ok=True)
_VARIABLES_FILE = _WORKER_ROOT / "variables.yml"
_VARIABLES_FILE.write_text(f"$apache_storage: {_LOCAL_STORAGE}\n")

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_RUNTIME"] = "tests/spark/apache/runtime"
os.environ["FABRICKS_TEST_SPARK_DEFAULT_PARALLELISM"] = "2"
os.environ["FABRICKS_TEST_DISPOSABLE_STORAGE"] = str(_LOCAL_STORAGE)
os.environ["FABRICKS_TEST_EXPECTED_CACHE"] = str(_EXPECTED_CACHE)
os.environ["FABRICKS_CONFIG"] = "tests/spark/apache/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_VARIABLE"] = str(_VARIABLES_FILE)
os.environ["FABRICKS_ENVIRONMENT"] = "docker"
os.environ["FABRICKS_IS_DEBUGMODE"] = "FALSE"
os.environ["FABRICKS_LOGLEVEL"] = "WARNING"
# Real get_job()/get_step() resolution against this file's bronze/silver
# blocks below, instead of falling back to `select * from fabricks.*_jobs`
# (a catalog table this suite never populates). Same flag tests/unit/config/
# already sets, against the same conf.fabricks.yml -- this just lets the
# same job/step config resolve against a real Spark+Delta session here too.
os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "TRUE"

shutil.rmtree(_LOCAL_STORAGE, ignore_errors=True)

# get_spark()'s docker branch (enableHiveSupport(), no spark.sql.warehouse.dir
# override - Task 4, out of scope here) defaults Hive's embedded Derby
# metastore and warehouse to CWD. Without this, table registration from a
# previous run leaks into the next one, since the repo working tree persists.
#
# Can't just chdir() each worker into its own directory here: pytest-xdist
# workers re-run collection themselves against the CLI's test-path argument,
# and a chdir at conftest import time (before that argument is resolved)
# makes every worker collect 0 items. So CWD stays unchanged for everyone,
# and per-worker isolation instead goes through JVM/Spark config: Derby's
# embedded metastore resolves its (relative, by default) database directory
# against the `derby.system.home` system property rather than CWD, and
# `_JAVA_OPTIONS` is honored by any `java` process py4j launches.
os.environ["_JAVA_OPTIONS"] = f"-Dderby.system.home={_WORKER_ROOT}"
# spark.sql.warehouse.dir is a static config -- must reach get_spark()'s
# builder before getOrCreate(), a plain spark.conf.set() afterward raises
# CANNOT_MODIFY_STATIC_CONFIG. get_spark() picks this env var up itself.
os.environ["FABRICKS_TEST_WAREHOUSE_DIR"] = str(_WORKER_ROOT / "spark-warehouse")
for _leftover in ("metastore_db", "spark-warehouse"):
    shutil.rmtree(_WORKER_ROOT / _leftover, ignore_errors=True)
(_WORKER_ROOT / "derby.log").unlink(missing_ok=True)


from pyspark.sql import SparkSession  # noqa: E402

from fabricks.utils.spark import get_spark  # noqa: E402 - must follow the env-var setup above

_SPARK: SparkSession = get_spark()

# fabricks/metastore/table.py's Table._create() issues plain `create table
# ... location '...'` DDL with no `using delta` clause -- it relies entirely
# on the session's default table provider being Delta. Real Databricks
# Runtime configures that by default; get_spark()'s "docker" branch does not.
_SPARK.sql("set spark.sql.sources.default = delta")
# ponytail: fixtures are a handful of rows, but Spark's production-scale
# partition defaults add pure per-test overhead here.
_SPARK.conf.set("spark.sql.shuffle.partitions", "2")
_SPARK.conf.set("spark.databricks.delta.snapshotPartitions", "2")
_SPARK.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "false")

# Database(name, spark=...).create() adds no location or property setup.
for _db_name in ("bronze", "silver", "gold", "expected", "cdc", "fabricks"):
    _SPARK.sql(f"create database if not exists {_db_name}")


class _Timer:
    """Diagnostic-only: prints elapsed wall time per stage inside
    king_and_queen_built's _build(). Not test infrastructure to keep long
    term -- remove once the per-stage cost is understood/addressed.
    """

    def __init__(self, iter_num: int, cdc: str):
        self.iter_num = iter_num
        self.cdc = cdc
        self._t0 = time.perf_counter()

    def lap(self, label: str) -> None:
        elapsed = time.perf_counter() - self._t0
        print(f"[TIMING] iter {self.iter_num} {self.cdc}: {label} at +{elapsed:.1f}s", flush=True)


def pytest_collection_modifyitems(items):
    """Automatically add 'apache' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        try:
            if Path(item.fspath).is_relative_to(root):
                item.add_marker(pytest.mark.apache)
        except (ValueError, AttributeError):
            if "apache" in str(item.fspath):
                item.add_marker(pytest.mark.apache)


@pytest.fixture(scope="session")
def local_spark():
    yield _SPARK
    _SPARK.stop()


@pytest.fixture(scope="session")
def king_and_queen_built(local_spark):
    """Factory fixture: returns a callable that builds bronze/CDC target tables
    in isolation — no job classes, no job config, no get_job().

    `_build(seed_from, iters, cdc)` seeds a fresh, uniquely-named target
    table directly from iteration `seed_from`'s own known-correct `expected`
    output (see seed_table in tests/spark/expected/compare.py), then
    runs one real SCD1/SCD2.update() call per iteration number in `iters`,
    in order, against that same table — each call's output becomes the next
    call's input, with no reseeding in between. `seed_from=0` means "no
    seed" — the from-empty-table bulk-load path, same as iteration 1 always
    was.

    `iters=[N]` (a single iteration) is the common case: seed from iteration
    N-1's expected state, run iteration N once, compare to iteration N's
    expected state. But `iters` can span a real multi-batch range (e.g.
    seed_from=3, iters=[4, 5, 6, 7]) to test that chaining several real
    merges back-to-back — without an oracle reseed between them — still
    lands on the right final state; or seed_from=0 with the full iteration
    range to test the whole chain from scratch in one go.

    This is deliberately NOT "seed from the actual table iteration
    seed_from's own test left behind": each call gets its own table, so a bug in one
    scenario's merge can never corrupt what another scenario's test sees —
    every scenario seeds from `expected` state, not another test's *actual*
    one. That's what makes calls to this fixture order-free and safe to
    parametrize/run in any order (an earlier version of this fixture
    required strictly increasing N against one shared live table across the
    whole test session; this version has no such requirement).

    scd1 and scd2 are built fully independently, including their own bronze
    tables — nothing about scd2's build depends on scd1 having run, or vice
    versa, so a caller that only needs one type never pays for building the
    other.

    Outer fixture is session-scoped (creates the `expected` views once,
    idempotently); the returned `_build` callable is invoked per scenario.
    See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #5.
    """
    from fabricks.cdc.scd1 import SCD1
    from fabricks.cdc.scd2 import SCD2
    from tests.spark.expected.compare import (
        create_expected_views,
        load_expected_scd1_seed,
        load_expected_scd2_seed,
        seed_table,
    )

    create_expected_views(local_spark, "scd2")
    create_expected_views(local_spark, "scd1")

    # __key = md5(array_join(array(id::string, __source::string), '*', '-1'))
    # (fabricks/cdc/templates/macros/hash.sql.jinja's add_key(fields=keys)) —
    # keys="id" always gets __source auto-appended by
    # Processor.get_query_context() once has_source is true, so seeding must
    # hash over both fields to match what a real merge call would compute.
    key_fields = ["id", "__source"]
    # Memoized per (seed_from, iters, cdc): both test functions parametrize
    # over every scenario for their own cdc type only, so nothing actually
    # repeats a call today, but this still protects against a future caller
    # requesting the same scenario twice in one session.
    _cache: dict[tuple[int, tuple[int, ...], str], object] = {}

    def _build(seed_from: int, iters: list[int], cdc: str):
        cache_key = (seed_from, tuple(iters), cdc)
        if cache_key in _cache:
            return _cache[cache_key]

        suffix = f"king_and_queen_seed{seed_from}_{iters[0]}to{iters[-1]}_{cdc}"
        scd = (
            SCD2("cdc", suffix, "scd2", spark=local_spark)
            if cdc == "scd2"
            else SCD1("cdc", suffix, "scd1", spark=local_spark)
        )

        if seed_from > 0:
            if cdc == "scd2":
                # expected's own scd2 schema has no __timestamp column either
                # -- same gap as scd1's, see load_expected_scd2_seed's
                # docstring.
                seed_table(scd.table, load_expected_scd2_seed(local_spark, seed_from), keys=key_fields)
            else:
                # scd1's own expected view has no __timestamp column (dropped
                # by its own oracle SQL) -- current.sql.jinja's __current CTE
                # needs it back off the real target on the next merge. See
                # load_expected_scd1_seed's docstring.
                seed_table(scd.table, load_expected_scd1_seed(local_spark, seed_from), keys=key_fields)

        # Each iteration in `iters` runs as a real, sequential
        # SCD1/SCD2.update() call against whatever this loop already left in
        # the table -- exactly what chaining several real merges
        # back-to-back looks like in production, just without any job
        # orchestration around it. For a single-iteration scenario (the
        # common case) this loop body runs once.
        for iter_num in iters:
            timer = _Timer(iter_num, cdc)
            fixtures_root = Path(__file__).resolve().parent / "fixtures" / f"iter{iter_num}"

            iter_dfs = []
            for entity in ("king", "queen"):
                # spark.read.json(path), not createDataFrame(list-of-dicts):
                # delete-log rows (see Task 8b) omit `name`/`doubleField`, so
                # the per-row column set genuinely varies within a file.
                # createDataFrame's list-of-dicts schema inference raises
                # PySparkValueError(CANNOT_DETERMINE_TYPE) when rows disagree
                # on columns (verified empirically against this container's
                # real Spark session) -- Spark's JSON-file reader uses a
                # different, more lenient inference path that unions the
                # per-row schemas instead of failing.
                #
                # No physical bronze Delta write here (there was one, dropped):
                # nothing in this test ever reads a bronze table back — the
                # SILVER merge below uses this iteration's own in-memory `df`
                # directly, not any accumulated bronze state — so writing one
                # was pure decoration, paid twice per iteration once scd1/scd2
                # were split into independent builds (~8s/iteration for a
                # write nothing consumes).
                path = fixtures_root / f"bronze_{entity}.jsonl"
                if not path.exists():
                    # iter11's queen: generate_fixtures.py skips writing a
                    # file when an entity has zero rows that iteration (no
                    # plain data, no deletelog) rather than writing an empty
                    # one spark.read.json() can't infer a schema from. Simply
                    # not contributing this entity's rows to `combined`
                    # below is the correct behavior, not a special case:
                    # the target table's existing rows for this entity
                    # (from an earlier iteration) are untouched by a merge
                    # whose incoming batch has nothing for them, exactly like
                    # a real incremental run where an entity has no new data.
                    timer.lap(f"{entity} bronze skipped (no data this iteration)")
                    continue
                df = local_spark.read.json(str(path))
                iter_dfs.append(df)
                timer.lap(f"{entity} bronze read")

            combined = iter_dfs[0]
            for df in iter_dfs[1:]:
                combined = combined.unionByName(df, allowMissingColumns=True)

            # One .update() call, against whatever state (real, for
            # iteration 1; seeded from `expected` or left by this loop's own
            # prior iteration, otherwise) already sits in the table.
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
            # iteration 1's king batch-1 (id=1, id=2) and queen batch-1
            # (id=101) all share the identical batch-derived timestamp
            # "2022-01-01T00:01:00", so all three legitimately become
            # 1900-01-01 -- not a one-row special case).
            #
            # update_schema() before update(), when the table already exists
            # (skipped on each CDC object's first-ever call, where
            # create_table() already builds the schema fresh from that
            # iteration's own data): real Silver jobs widen the target
            # table's schema between incremental runs (this plan
            # deliberately has no such orchestration step -- see the "no
            # update_schema() between jobs" note in fabricks/utils/spark.py's
            # docker branch). Without it, iteration 2's newField column
            # (present in its own source rows but absent from the
            # iteration-1-created table) breaks the merge query's own
            # rectify CTE (fabricks/cdc/templates/ctes/rectify.sql.jinja's
            # __rectified_base, which selects `intermediates` -- including
            # newField once any iteration introduces it -- by name from
            # *both* the incoming batch and `__current`, a CTE reading the
            # existing target table's own rows): verified UNRESOLVED_COLUMN
            # against this container's real Spark session, listing only the
            # target table's pre-existing 5 columns.
            # autoMerge (spark.databricks.delta.schema.autoMerge.enabled)
            # only patches the final `merge into` statement -- it doesn't
            # retroactively widen an arbitrary SELECT reading the
            # not-yet-altered target table mid-query. update_schema() (an
            # existing, already-public Generator method -- not new
            # production code) issues the real ALTER TABLE ADD COLUMNS
            # ahead of time, exactly what real job orchestration does
            # between runs.
            # Skip the call entirely unless the incoming batch actually has a
            # column the seeded table doesn't -- verified this is genuinely rare
            # across all 9 iterations' real fixture data: only iteration 2
            # introduces a new column (`newField`), and it's present in every
            # iteration's data from iteration 2 onward, so iterations 3-9 seed
            # from a table that already has it and update_schema() would be a
            # pure no-op there (confirmed: ~35-69s per call, by far the most
            # expensive single stage measured). Diffing column *names* only,
            # cheap relative to the ALTER TABLE it guards -- if a future
            # iteration re-introduces schema drift this still catches it, no
            # hardcoded iteration number to keep in sync with the fixture data.
            if scd.table.exists() and set(combined.columns) - set(scd.table.columns):
                if cdc == "scd2":
                    scd.update_schema(combined, keys="id", add_key=True, soft_delete=True, correct_valid_from=True)
                else:
                    scd.update_schema(combined, keys="id", add_key=True, soft_delete=True)
            timer.lap("update_schema done")

            if cdc == "scd2":
                scd.update(combined, keys="id", add_key=True, soft_delete=True, correct_valid_from=True)
            else:
                scd.update(combined, keys="id", add_key=True, soft_delete=True)
            timer.lap(f"{cdc}.update() done")

        _cache[cache_key] = scd
        return scd

    return _build


@pytest.fixture(scope="session")
def king_and_queen_registered_sources(local_spark):
    """Seeds the two Delta tables tests/spark/apache/runtime/bronze/_config.{kings,queens}.yml's
    `register`-mode jobs read from (their `uri`), independent of anything
    king_and_queen_built creates under its own `cdc` database. Session-scoped
    + written once: `register` mode's source table is read-only from the
    job's own point of view -- reuses the same iter1 NDJSON fixture files
    king_and_queen_built already reads, no new fixture data.

    Writing the Delta files at `uri` isn't enough: Bronze.parse(stream=False)
    (fabricks/core/jobs/bronze.py) reads `select * from {qualified_name}`, not
    from `uri` directly, so the job's own catalog table
    (bronze.king_scd1/bronze.queen_scd1) must exist too -- register_external_table()
    is the framework's own mechanism for that (`create table if not exists ...
    location '<uri>'`).
    """
    from pyspark.sql.functions import col

    from fabricks.core import get_job
    from fabricks.utils.path import resolve_fileshare_path

    fixtures_root = Path(__file__).resolve().parent / "fixtures" / "iter1"
    for entity in ("king", "queen"):
        path = resolve_fileshare_path(str(_LOCAL_STORAGE / "bronze_external" / entity))
        df = local_spark.read.json(str(fixtures_root / f"bronze_{entity}.jsonl"))
        # register_external_table() asserts __timestamp is TimestampType; the JSON
        # reader infers it as string from the fixture's ISO-8601 literals.
        df = df.withColumn("__timestamp", col("__timestamp").cast("timestamp"))
        df.write.format("delta").mode("overwrite").save(path.string)

        get_job(step="bronze", topic=entity, item="scd1").register_external_table()
