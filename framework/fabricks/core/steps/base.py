import logging
from functools import cached_property
from typing import Iterable, List, Literal, Optional, Tuple, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, md5
from pyspark.sql.types import Row
from sparkdantic import create_spark_schema

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
from fabricks.core.jobs.get_job import get_job_internal
from fabricks.core.jobs.get_jobs import get_jobs_sorted
from fabricks.core.read import read_yaml
from fabricks.core.steps._types import JobResult, Modes, Timeouts
from fabricks.core.steps.get_step_conf import get_step_conf
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table
from fabricks.models import SchemaDependencies
from fabricks.models.cdc import CdcContext
from fabricks.utils.helpers import run_in_parallel


class BaseStep:
    def __init__(self, step: str):
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
    def workers(self):
        w = self.options.workers

        if w is None:
            w = CONF_RUNTIME.options.workers

        assert w is not None

        return cast(int, w)

    def _get_timeout(self, what: str) -> int:
        t = getattr(self.options.timeouts, what, None)

        if t is None:
            t = getattr(CONF_RUNTIME.options.timeouts, what)

        assert t is not None

        return int(t)

    @cached_property
    def timeouts(self) -> Timeouts:
        return Timeouts(
            job=self._get_timeout("job"),
            step=self._get_timeout("step"),
        )

    @cached_property
    def conf(self) -> dict:
        return STEPS[self.name].model_dump()

    @cached_property
    def options(self):
        return STEPS[self.name].options

    def drop(self):
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

        try:
            SPARK.sql(f"delete from fabricks.steps where step = '{self}'")
        except Exception:
            pass

        self.database.drop()

    def create(self, mode: Optional[Modes] = "parallel", max_retries: Optional[int] = 2):
        DEFAULT_LOGGER.info("create", extra={"label": self})

        if not self.runtime.exists():
            DEFAULT_LOGGER.warning(f"could not find {self.name} in runtime")
        else:
            self.update(mode=mode, max_retries=max_retries)

    def update(
        self,
        mode: Optional[Modes] = "parallel",
        update_dependencies: Optional[bool] = True,
        progress_bar: Optional[bool] = False,
        incremental: Optional[bool] = False,
        max_retries: Optional[int] = 2,
    ):
        if not self.runtime.exists():
            DEFAULT_LOGGER.warning(f"could not find {self.name} in runtime")
            return

        if not self.database.exists():
            self.database.create()

        self.update_configurations()
        self.create_db_objects(mode=mode, incremental=incremental, update_lists=False, max_retries=max_retries)

        if update_dependencies:
            self.update_dependencies(progress_bar=progress_bar)

        self.update_tables_list()
        self.update_views_list()
        self.update_steps_list()

    def _get_dependencies(
        self,
        progress_bar: Optional[bool] = False,
        topic: Optional[Union[str, List[str]]] = None,
        include_manual: Optional[bool] = False,
        loglevel: Optional[Literal[10, 20, 30, 40, 50]] = None,
    ) -> Tuple[DataFrame, List[JobResult]]:
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

        if df.isEmpty():
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
            if res.error:
                errors.append(res)
            elif res.dependencies:
                dependencies.extend(res.dependencies)

        df = SPARK.createDataFrame([d.model_dump() for d in dependencies], SchemaDependencies)

        return df, errors

    def _dispatch(self, mode: Optional[Modes], df: DataFrame) -> List[JobResult]:
        if mode == "parallel":
            return self._create_in_parallel(df)

        return self._create_sequentially(df)

    def _create_in_parallel(self, df: DataFrame) -> List[JobResult]:
        return run_in_parallel(
            _create_db_object,
            df,
            workers=16,
            progress_bar=True,
            logger=DEFAULT_LOGGER,
            loglevel=logging.CRITICAL,
        )

    def _create_sequentially(self, df: DataFrame) -> List[JobResult]:
        try:
            deps_df, dep_errors = self._get_dependencies(loglevel=logging.CRITICAL)

            if dep_errors:
                DEFAULT_LOGGER.warning(
                    f"could not get some dependencies for sorting ({len(dep_errors)} error(s))",
                    extra={"label": self},
                )

            sorted_df = get_jobs_sorted(df, deps_df)
        except Exception as e:
            DEFAULT_LOGGER.warning(
                f"could not sort jobs by dependencies due to error: {e}",
                extra={"label": self},
            )
            sorted_df = df

        result = []

        for row in sorted_df.collect():
            res = _create_db_object(row)
            result.append(res)

        return result

    def get_jobs_iter(self, topic: Optional[str] = None) -> Iterable[dict]:
        """Yield job configurations from YAML files with variable substitution."""
        return read_yaml(self.runtime, root="job", preferred_file_name=topic)

    def get_jobs(self, topic: Optional[str] = None) -> DataFrame:
        DEFAULT_LOGGER.debug("get jobs", extra={"label": self})

        try:
            conf = get_step_conf(self.name)
            schema = create_spark_schema(conf)
            jobs = self.get_jobs_iter(topic=topic)
            df = SPARK.createDataFrame(jobs, schema=schema)
            df = df.withColumn("job_id", md5(expr("concat(step, '.' ,topic, '_', item)")))

            df.cache()

            duplicated_df = df.groupBy("job_id", "step", "topic", "item").count().where("count > 1")
            rows = duplicated_df.collect()

            if rows:
                duplicates = ",".join(f"{row.step}.{row.topic}_{row.item}" for row in rows)
                raise AssertionError(f"duplicated job(s) ({duplicates})")

            if df.isEmpty():
                raise ValueError("no jobs found")

            return df
        except AssertionError as e:
            DEFAULT_LOGGER.exception("fail to get jobs", extra={"label": self})
            raise e

    def get_dependencies(
        self,
        progress_bar: Optional[bool] = False,
        topic: Optional[Union[str, List[str]]] = None,
        include_manual: Optional[bool] = False,
        loglevel: Optional[Literal[10, 20, 30, 40, 50]] = None,
    ) -> DataFrame:
        df, errors = self._get_dependencies(
            progress_bar=progress_bar,
            topic=topic,
            include_manual=include_manual,
            loglevel=loglevel,
        )
        _log_and_raise_errors(errors, "get dependencies")

        return df

    def create_db_objects(
        self,
        mode: Optional[Modes] = "parallel",
        max_retries: Optional[int] = 2,
        update_lists: Optional[bool] = True,
        incremental: Optional[bool] = False,
    ) -> None:
        df = self.get_jobs()

        if incremental:
            table_df = self.database.get_tables()
            view_df = self.database.get_views()
            df = df.join(table_df, "job_id", how="left_anti")
            df = df.join(view_df, "job_id", how="left_anti")

        results = self._dispatch(mode, df)
        errors = [res for res in results if res.error]
        error_count: int = len(errors)
        attempt = 0
        DEFAULT_LOGGER.debug(
            f"{len(results) - error_count} db objects created, {error_count} error(s) remaining",
            extra={"label": self},
        )

        while errors and max_retries and attempt < max_retries:
            attempt += 1
            DEFAULT_LOGGER.warning(
                f"retrying failed db objects, {max_retries - attempt + 1} retries left",
                extra={"label": self},
            )
            failed_job_ids = [e.job_id for e in errors]
            errors_df = df.where(df["job_id"].isin(failed_job_ids))
            results = self._dispatch(mode, errors_df)
            errors = [res for res in results if res.error]

            if len(errors) == error_count:
                DEFAULT_LOGGER.warning(
                    "no improvement in errors after retry, stop retries",
                    extra={"label": self},
                )
                break
            else:
                error_count = len(errors)
                DEFAULT_LOGGER.debug(
                    f"{error_count} db objects still not created, retrying...",
                    extra={"label": self},
                )

        if update_lists:
            self.update_tables_list()
            self.update_views_list()

        _log_and_raise_errors(errors, "create db objects")

    def update_dependencies(
        self,
        progress_bar: Optional[bool] = False,
        topic: Optional[Union[str, List[str]]] = None,
        include_manual: Optional[bool] = False,
        loglevel: Optional[Literal[10, 20, 30, 40, 50]] = None,
    ) -> None:
        df, errors = self._get_dependencies(
            progress_bar=progress_bar,
            topic=topic,
            include_manual=include_manual,
            loglevel=loglevel,
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
                df,
                context=CdcContext(keys=["dependency_id"], update_where=update_where),
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
                df,
                context=CdcContext(keys=["dependency_id"], update_where=update_where, uuid=True),
            )

        _log_and_raise_errors(errors, "update dependencies")

    def register(self, update: Optional[bool] = False, drop: Optional[bool] = False):
        if drop:
            SPARK.sql(f"drop database if exists {self.name} cascade ")
            SPARK.sql(f"create database {self.name}")

        if update:
            self.update_configurations()

        df = self.get_jobs()

        if not df.isEmpty():
            table_df = self.database.get_tables()

            if not table_df.isEmpty():
                df = df.join(table_df, "job_id", how="left_anti")

        if not df.isEmpty():
            DEFAULT_LOGGER.setLevel(logging.CRITICAL)
            run_in_parallel(_register, df, workers=16, progress_bar=True, run_as="Pool")
            DEFAULT_LOGGER.setLevel(LOGLEVEL)

    def update_steps_list(self):
        order = self.options.order or 0
        df = SPARK.sql(f"select '{self.expand}' as expand, '{self.name}' as step, '{order}' :: int as `order`")
        NoCDC("fabricks", "steps").delete_missing(
            df, context=CdcContext(keys=["step"], update_where=f"step = '{self.name}'")
        )

    def update_views_list(self):
        df = self.database.get_views()
        df = df.withColumn("job_id", expr("md5(view)"))
        DEFAULT_LOGGER.info("update views list", extra={"label": self})
        NoCDC("fabricks", self.name, "views").delete_missing(df, context=CdcContext(keys=["job_id"]))

    def update_tables_list(self):
        df = self.database.get_tables()
        df = df.withColumn("job_id", expr("md5(table)"))
        DEFAULT_LOGGER.info("update tables list", extra={"label": self})
        NoCDC("fabricks", self.name, "tables").delete_missing(df, context=CdcContext(keys=["job_id"]))

    def update_lists(self):
        self.update_tables_list()
        self.update_views_list()
        self.update_steps_list()

    def update_configurations(self, drop: Optional[bool] = False):
        df = self.get_jobs()
        DEFAULT_LOGGER.info("update configurations", extra={"label": self})
        cdc = NoCDC("fabricks", self.name, "jobs")

        if drop:
            cdc.table.drop()
        elif cdc.table.exists():
            df_diffs = cdc.get_differences_with_deltatable(df, context=CdcContext())

            if not df_diffs.isEmpty():
                DEFAULT_LOGGER.warning("schema drift detected", extra={"label": self})
                cdc.table.overwrite_schema(df=df)

        cdc.delete_missing(df, context=CdcContext(keys=["job_id"]))

    def __str__(self):
        return self.name


def _log_and_raise_errors(errors: List[JobResult], action: str) -> None:
    if errors:
        for e in errors:
            DEFAULT_LOGGER.warning(f"fail to {action}", extra={"label": e.job})

        raise ValueError(f"fail to {action} - {len(errors)} failure(s), check logs for details")


# to avoid AttributeError: can't pickle local object
def _get_dependencies(row: Row) -> JobResult:
    j = row["job"] or f"{row['step']}.{row['topic']}_{row['item']}"

    try:
        job = get_job_internal(step=row["step"], job_id=row["job_id"], conf=row)
        return JobResult(job=j, dependencies=job.get_dependencies())
    except Exception as e:
        DEFAULT_LOGGER.warning("fail to get dependencies", extra={"label": j})
        return JobResult(job=j, error=e)


def _create_db_object(row: Row) -> JobResult:
    j = row["job"] or f"{row['step']}.{row['topic']}_{row['item']}"

    try:
        job = get_job_internal(step=row["step"], job_id=row["job_id"], conf=row)
        job.create()
        return JobResult(job=j, job_id=row["job_id"])
    except Exception as e:  # noqa E722
        DEFAULT_LOGGER.warning("fail to create db object", extra={"label": j})
        return JobResult(job=j, job_id=row["job_id"], error=e)


def _register(row: Row) -> JobResult:
    j = row["job"] or f"{row['step']}.{row['topic']}_{row['item']}"

    try:
        job = get_job_internal(step=row["step"], topic=row["topic"], item=row["item"])
        job.register()
        return JobResult(job=j)
    except Exception as e:
        DEFAULT_LOGGER.warning("fail to register job", extra={"label": j})
        return JobResult(job=j, error=e)
