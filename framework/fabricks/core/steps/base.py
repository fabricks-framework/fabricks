from collections.abc import Iterable
import contextlib
from functools import cached_property
import logging
import re
from typing import Any, Literal

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, md5
from pyspark.sql.types import Row
from sparkdantic import create_spark_schema
from typing_extensions import deprecated

from fabricks.cdc import NoCDC
from fabricks.context import (
    CONF_RUNTIME,
    LOGLEVEL,
    PATHS_RUNTIME,
    PATHS_STORAGE,
    SPARK,
    STEPS,
    Bronzes,
    Golds,
    Silvers,
)
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.get_job import get_job, get_job_internal
from fabricks.core.read import read_yaml
from fabricks.core.steps._types import Timeouts
from fabricks.core.steps.get_step_conf import get_step_conf
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table
from fabricks.models import SchemaDependencies, StepBronzeOptions, StepGoldOptions, StepSilverOptions
from fabricks.utils.helpers import run_in_parallel


class BaseStep:
    def __init__(self, step: str) -> None:
        self.name = step

        if self.name in Bronzes:
            self.expand = "bronze"
        elif self.name in Silvers:
            self.expand = "silver"
        elif self.name in Golds:
            self.expand = "gold"

        else:
            raise ValueError(self.name, "does not expand a default step")

        _storage = PATHS_STORAGE.get(self.name)
        assert _storage
        _runtime = PATHS_RUNTIME.get(self.name)
        assert _runtime

        self.spark = SPARK
        self.storage = _storage
        self.runtime = _runtime
        self.database = Database(self.name)

    @cached_property
    def workers(self) -> int:
        w = self.options.workers
        if w is None:
            w = CONF_RUNTIME.options.workers
        assert w is not None
        return w

    def _get_timeout(self, what: str) -> int:
        t = getattr(self.options.timeouts, what, None)
        if t is None:
            t = getattr(CONF_RUNTIME.options.timeouts, what)
        assert t is not None
        return int(t)

    @cached_property
    def timeouts(self) -> Timeouts:
        return Timeouts(job=self._get_timeout("job"), step=self._get_timeout("step"))

    @cached_property
    def conf(self) -> dict:
        return STEPS[self.name].model_dump()

    @cached_property
    def options(self) -> StepBronzeOptions | StepSilverOptions | StepGoldOptions:
        return STEPS[self.name].options

    def drop(self) -> None:
        DEFAULT_LOGGER.warning("drop", extra={"label": self})

        fs = self.database.storage
        assert fs

        tmp = fs.joinpath("tmp")
        if tmp.exists():
            DEFAULT_LOGGER.debug("clean tmp folder", extra={"label": self})
            tmp.rm()

        checkpoint = fs.joinpath("checkpoints")
        if checkpoint.exists():
            DEFAULT_LOGGER.debug("clean checkpoint folder", extra={"label": self})
            checkpoint.rm()

        schema = fs.joinpath("schemas")
        if schema.exists():
            DEFAULT_LOGGER.debug("clean schema folder", extra={"label": self})
            schema.rm()

        DEFAULT_LOGGER.debug("clean fabricks", extra={"label": self})
        for t in ["jobs", "tables", "dependencies", "views"]:
            tbl = Table("fabricks", self.name, t)
            tbl.drop()

        with contextlib.suppress(Exception):
            SPARK.sql(f"delete from fabricks.steps where step = '{self}'")

        self.database.drop()

    def create(self) -> None:
        DEFAULT_LOGGER.info("create", extra={"label": self})

        if not self.runtime.exists():
            DEFAULT_LOGGER.warning(f"could not find {self.name} in runtime")
        else:
            self.update()

    def update(
        self,
        update_dependencies: bool | None = True,
        progress_bar: bool | None = False,
        incremental: bool | None = False,
        max_registration_attempts: int = 3,
    ) -> None:
        if not self.runtime.exists():
            DEFAULT_LOGGER.warning(f"could not find {self.name} in runtime")
            return

        if not self.database.exists():
            self.database.create()

        self.update_configurations()

        all_errors = []

        # Collect errors from create_db_objects
        _, create_errors = self._create_db_objects_internal(
            incremental=incremental, update_lists=False, max_attempts=max_registration_attempts
        )
        all_errors.extend(create_errors)

        # Collect errors from update_dependencies
        if update_dependencies:
            _, dep_errors = self._update_dependencies_internal(progress_bar=progress_bar)
            all_errors.extend(dep_errors)

        self.update_tables_list()
        self.update_views_list()
        self.update_steps_list()

        if all_errors:
            _log_and_raise_errors(all_errors, "update step")

    # ========== Internal Methods ==========
    # Private methods that return (result, errors) tuples for flexible error handling

    def _get_dependencies_internal(
        self,
        progress_bar: bool | None = False,
        topic: str | list[str] | None = None,
        include_manual: bool | None = False,
        loglevel: Literal[10, 20, 30, 40, 50] | None = None,  # noqa: ARG002 - kept to match public wrapper, called with loglevel= by get_dependencies
    ) -> tuple[DataFrame, list[dict]]:
        """Private version that returns (df, errors) instead of raising."""
        DEFAULT_LOGGER.debug("get dependencies", extra={"label": self})

        df = self.get_jobs()

        if not include_manual:
            df = df.where("not options.type <=> 'manual'")

        if topic:
            if isinstance(topic, str):
                topic = [topic]

            where = ", ".join([f"'{t}'" for t in topic])
            DEFAULT_LOGGER.debug(f"where topic in {where}", extra={"label": self})
            df = df.where(f"topic in ({where})")

        if not df:
            raise ValueError("no jobs found")

        results = run_in_parallel(
            _get_dependencies,
            df,
            workers=16,
            progress_bar=progress_bar,
            logger=DEFAULT_LOGGER,
            loglevel=logging.CRITICAL,
        )

        errors = []
        dependencies = []

        for res in results:
            if res.get("error"):
                errors.append(res)
            elif res.get("dependencies"):
                dependencies.extend(res.get("dependencies"))

        df = SPARK.createDataFrame([d.model_dump() for d in dependencies], SchemaDependencies)
        return df, errors

    def _create_db_objects_internal(
        self,
        retry: bool | None = True,
        update_lists: bool | None = True,
        incremental: bool | None = False,
        max_attempts: int = 3,
        parallel: bool | None = True,
    ) -> tuple[DataFrame | None, list[dict]]:
        """Private version that returns (df, errors) instead of raising."""

        def _create_db_objects(df: DataFrame, *, workers: int) -> list[dict]:
            DEFAULT_LOGGER.info("create db objects", extra={"label": self})
            results = run_in_parallel(
                _create_db_object,
                df,
                workers=workers,
                progress_bar=True,
                logger=DEFAULT_LOGGER,
                loglevel=logging.CRITICAL,
            )
            errors = [res for res in results if res.get("error")]
            DEFAULT_LOGGER.debug(
                f"{len(results) - len(errors)} db objects created, {len(errors)} errors", extra={"label": self}
            )
            return errors

        df = self.get_jobs()

        if incremental:
            table_df = self.database.get_tables()
            view_df = self.database.get_views()

            df = df.join(table_df, "job_id", how="left_anti")
            df = df.join(view_df, "job_id", how="left_anti")

        df = df.cache()

        errors = []
        if df:
            errors = _create_db_objects(df, workers=16 if parallel else 1)

        # Batches with inter-item dependencies (e.g. memory-mode views
        # referencing each other) race when dispatched together: an item
        # can be attempted before the item it depends on has been
        # registered. See https://github.com/fabricks-framework/fabricks/
        # issues/183. Each failure's error message names the missing
        # table/view, which tells us which sibling it's actually blocked
        # on -- so retries are ordered by that instead of blindly
        # re-dispatching the whole failing set again and hoping. Computing
        # the real dependency graph up front (Job.get_dependencies()) would
        # be more precise, but it's expensive for notebook-backed jobs
        # (needs a real `spark.sql("explain extended...")` per job) and
        # would pay that cost on every run just to guard a rare race --
        # this stays reactive, only doing extra work when a race actually
        # happens. An item whose blocker is itself still failing is left
        # for a later pass; a chain of depth N resolves within N passes.
        # Items ready in the same pass aren't blocked on each other, so
        # they're still dispatched together, in parallel.
        if errors and retry:
            DEFAULT_LOGGER.warning("retry enabled", extra={"label": self})
            name_to_job_id = {
                f"{row['topic']}_{row['item']}": row["job_id"]
                for row in df.select("topic", "item", "job_id").collect()
            }

            prev_failed_ids: frozenset[str] | None = None
            attempt = 1
            while errors and attempt < max_attempts:
                failed_ids = frozenset(e["job_id"] for e in errors if e.get("job_id"))
                if failed_ids == prev_failed_ids:
                    break
                prev_failed_ids = failed_ids

                blocked = {
                    e["job_id"]
                    for e in errors
                    if e.get("job_id") and _referenced_job_id(e.get("error"), name_to_job_id) in failed_ids
                }
                ready_ids = failed_ids - blocked
                if not ready_ids:
                    break  # every remaining failure is blocked on another failure: circular or stuck

                retry_df = df.where(df["job_id"].isin(list(ready_ids)))
                workers = 16 if parallel else 1
                new_errors = _create_db_objects(retry_df, workers=workers)
                errors = new_errors + [e for e in errors if e.get("job_id") in blocked]
                attempt += 1

        df.unpersist()

        # Runs after retries so objects created on a retry pass are still
        # picked up (see https://github.com/fabricks-framework/fabricks/
        # issues/183) -- listing before retries resolved would silently
        # drop anything the retry loop had to register.
        if update_lists:
            self.update_tables_list()
            self.update_views_list()

        return df, errors

    def _update_dependencies_internal(
        self,
        progress_bar: bool | None = False,
        topic: str | list[str] | None = None,
        include_manual: bool | None = False,
        loglevel: Literal[10, 20, 30, 40, 50] | None = None,
    ) -> tuple[DataFrame, list[dict]]:
        """Private version that returns (df, errors) instead of raising."""
        df, errors = self._get_dependencies_internal(
            progress_bar=progress_bar, topic=topic, include_manual=include_manual, loglevel=loglevel
        )
        df.cache()

        DEFAULT_LOGGER.info("update dependencies", extra={"label": self})

        update_where = None

        if topic is None:
            if not include_manual:
                update_where = (
                    f"job_id not in (select job_id from fabricks.{self.name}_jobs where not options.type <=> 'manual')"
                )

            if update_where:
                DEFAULT_LOGGER.debug(f"update where {update_where}", extra={"label": self})

            NoCDC("fabricks", self.name, "dependencies").delete_missing(
                df, keys=["dependency_id"], update_where=update_where
            )

        else:
            if isinstance(topic, str):
                topic = [topic]

            where_topic = f"""topic in ('{"', '".join(topic)}')"""
            where_not_manual = "-- manual job(s) included"
            if not include_manual:
                where_not_manual = "and not options.type <=> 'manual'"

            update_where = (
                f"""job_id in (select job_id from fabricks.{self.name}_jobs where {where_topic} {where_not_manual})"""
            )
            DEFAULT_LOGGER.debug(f"update where {update_where}", extra={"label": self})

            NoCDC("fabricks", self.name, "dependencies").delete_missing(
                df, keys=["dependency_id"], update_where=update_where, uuid=True
            )

        return df, errors

    # ========== Public API Methods ==========

    def get_jobs_iter(self, topic: str | None = None) -> Iterable[dict]:
        """Yield job configurations from YAML files with variable substitution."""
        return read_yaml(self.runtime, root="job", preferred_file_name=topic)

    def get_jobs(self, topic: str | None = None) -> DataFrame:
        DEFAULT_LOGGER.debug("get jobs", extra={"label": self})

        try:
            conf = get_step_conf(self.name)
            schema = create_spark_schema(conf)
            jobs = self.get_jobs_iter(topic=topic)

            df = SPARK.createDataFrame(jobs, schema=schema)
            df = df.withColumn("job_id", md5(expr("concat(step, '.' ,topic, '_', item)")))

            # Collect once to avoid double scan
            duplicated_df = df.groupBy("job_id", "step", "topic", "item").count().where("count > 1")
            rows = duplicated_df.collect()
            if rows:
                duplicates = ",".join(f"{row.step}.{row.topic}_{row.item}" for row in rows)
                raise AssertionError(f"duplicated job(s) ({duplicates})")

            if not df:
                raise ValueError("no jobs found")

            return df

        except AssertionError as e:
            DEFAULT_LOGGER.exception("fail to get jobs", extra={"label": self})
            raise e

    def get_dependencies(
        self,
        progress_bar: bool | None = False,
        topic: str | list[str] | None = None,
        include_manual: bool | None = False,
        loglevel: Literal[10, 20, 30, 40, 50] | None = None,
    ) -> DataFrame:
        df, errors = self._get_dependencies_internal(
            progress_bar=progress_bar, topic=topic, include_manual=include_manual, loglevel=loglevel
        )
        _log_and_raise_errors(errors, "get dependencies", "jobs")
        return df

    def create_db_objects(
        self,
        retry: bool | None = True,
        update_lists: bool | None = True,
        incremental: bool | None = False,
        max_attempts: int = 3,
        parallel: bool | None = True,
    ) -> None:
        _, errors = self._create_db_objects_internal(
            retry=retry,
            update_lists=update_lists,
            incremental=incremental,
            max_attempts=max_attempts,
            parallel=parallel,
        )
        _log_and_raise_errors(errors, "create db objects", "objects")

    def update_dependencies(
        self,
        progress_bar: bool | None = False,
        topic: str | list[str] | None = None,
        include_manual: bool | None = False,
        loglevel: Literal[10, 20, 30, 40, 50] | None = None,
    ) -> None:
        _, errors = self._update_dependencies_internal(
            progress_bar=progress_bar, topic=topic, include_manual=include_manual, loglevel=loglevel
        )
        _log_and_raise_errors(errors, "update dependencies", "jobs")

    def register(self, update: bool | None = False, drop: bool | None = False) -> None:
        if drop:
            SPARK.sql(f"drop database if exists {self.name} cascade ")
            SPARK.sql(f"create database {self.name}")

        if update:
            self.update_configurations()

        df = self.get_jobs()
        if df:
            table_df = self.database.get_tables()
            if table_df:
                df = df.join(table_df, "job_id", how="left_anti")

        if df:
            DEFAULT_LOGGER.setLevel(logging.CRITICAL)
            run_in_parallel(_register, df, workers=16, progress_bar=True, run_as="Pool")
            DEFAULT_LOGGER.setLevel(LOGLEVEL)

    def update_steps_list(self) -> None:
        order = self.options.order or 0
        df = SPARK.sql(f"select '{self.expand}' as expand, '{self.name}' as step, '{order}' :: int as `order`")

        NoCDC("fabricks", "steps").delete_missing(df, keys=["step"], update_where=f"step = '{self.name}'")

    def update_views_list(self) -> None:
        df = self.database.get_views()
        df = df.withColumn("job_id", expr("md5(view)"))

        DEFAULT_LOGGER.info("update views list", extra={"label": self})
        NoCDC("fabricks", self.name, "views").delete_missing(df, keys=["job_id"])

    def update_tables_list(self) -> None:
        df = self.database.get_tables()
        df = df.withColumn("job_id", expr("md5(table)"))

        DEFAULT_LOGGER.info("update tables list", extra={"label": self})
        NoCDC("fabricks", self.name, "tables").delete_missing(df, keys=["job_id"])

    def update_configurations(self, drop: bool | None = False) -> None:
        df = self.get_jobs()

        DEFAULT_LOGGER.info("update configurations", extra={"label": self})

        cdc = NoCDC("fabricks", self.name, "jobs")

        if drop:
            cdc.table.drop()
        elif cdc.table.exists():
            df_diffs = cdc.get_differences_with_deltatable(df)
            if not df_diffs.isEmpty():
                DEFAULT_LOGGER.warning("schema drift detected", extra={"label": self})
                cdc.table.overwrite_schema(df=df)

        cdc.delete_missing(df, keys=["job_id"])

    # ========== Deprecated Methods ==========

    @deprecated("use create_db_objects instead")
    def create_jobs(self, retry: bool | None = True) -> None:
        return self.create_db_objects(retry=retry)

    @deprecated("use update_configurations instead")
    def update_jobs(self, drop: bool | None = False) -> None:
        return self.update_configurations(drop=drop)

    @deprecated("use update_tables_list instead")
    def update_tables(self) -> None:
        return self.update_tables_list()

    @deprecated("use update_views_list instead")
    def update_views(self) -> None:
        return self.update_views_list()

    def __str__(self) -> str:
        return self.name


def _log_and_raise_errors(errors: list[dict], action: str, object_type: str = "operations") -> None:
    if errors:
        logs = []
        for e in errors:
            DEFAULT_LOGGER.exception(f"fail to {action}", extra={"label": e["job"]}, exc_info=e["error"])
            logs.append(f"  {e['job']}: {type(e['error']).__name__}: {str(e['error']).splitlines()[0]}")

        raise ValueError(f"could not {action} - {len(errors)} {object_type} failed:\n" + "\n".join(logs))


_MISSING_VIEW_RE = re.compile(r"TABLE_OR_VIEW_NOT_FOUND[^`]*`\w+`\.`(\w+)`", re.IGNORECASE | re.DOTALL)


def _referenced_job_id(error: object, name_to_job_id: dict[str, str]) -> str | None:
    """The job_id a TABLE_OR_VIEW_NOT_FOUND error is blocked on, if any."""
    if error is None:
        return None
    match = _MISSING_VIEW_RE.search(str(error))
    if not match:
        return None
    return name_to_job_id.get(match.group(1))


# to avoid AttributeError: can't pickle local object
def _get_dependencies(row: Row) -> dict[str, Any]:
    job = get_job_internal(step=row["step"], job_id=row["job_id"], conf=row)
    try:
        return {"job": str(job), "dependencies": job.get_dependencies()}
    except Exception as e:
        DEFAULT_LOGGER.exception("fail to get dependencies", extra={"label": job})
        return {"job": str(job), "error": e}


def _create_db_object(row: Row) -> dict[str, Any]:
    job = get_job_internal(step=row["step"], job_id=row["job_id"], conf=row)
    try:
        job.create()
        return {"job": str(job), "job_id": row["job_id"]}
    except Exception as e:
        DEFAULT_LOGGER.exception("fail to create db object", extra={"label": job})
        return {"job": str(job), "job_id": row["job_id"], "error": e}


def _register(row: Row) -> dict[str, Any]:
    job = get_job(step=row["step"], topic=row["topic"], item=row["item"])
    try:
        job.register()
        return {"job": str(job)}
    except Exception as e:
        DEFAULT_LOGGER.exception("fail to get dependencies", extra={"label": job})
        return {"job": str(job), "error": e}
