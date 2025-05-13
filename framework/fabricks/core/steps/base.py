import logging
from typing import List, Optional, Tuple, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, md5
from pyspark.sql.types import Row

from fabricks.cdc import SCD1
from fabricks.context import CONF_RUNTIME, LOGLEVEL, PATHS_RUNTIME, PATHS_STORAGE, SPARK, STEPS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base._types import Bronzes, Golds, Silvers, TStep
from fabricks.core.jobs.get_job import get_job
from fabricks.core.steps._types import Timeouts
from fabricks.core.steps.get_step_conf import get_step_conf
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table
from fabricks.utils.helpers import concat_dfs, run_in_parallel
from fabricks.utils.read.read_yaml import read_yaml
from fabricks.utils.schema import get_schema_for_type


class BaseStep:
    def __init__(self, step: Union[TStep, str]):
        self.name = cast(str, step)

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

    _conf: Optional[dict] = None
    _options: Optional[dict] = None

    _workers: Optional[int] = None
    _timeouts: Optional[Timeouts] = None

    @property
    def workers(self):
        if not self._workers:
            w = self.options.get("workers")
            if w is None:
                w = CONF_RUNTIME.get("options", {}).get("workers")
            assert w is not None
            self._workers = cast(int, w)

        return self._workers

    def _get_timeout(self, what: str) -> int:
        t = self.options.get("timeouts", {}).get(what, None)
        if t is None:
            t = CONF_RUNTIME.get("options", {}).get("timeouts", {}).get(what)
        assert t is not None

        return int(t)

    @property
    def timeouts(self) -> Timeouts:
        if not self._timeouts:
            self._timeouts = Timeouts(
                job=self._get_timeout("job"),
                step=self._get_timeout("step"),
            )

        return self._timeouts

    @property
    def conf(self) -> dict:
        if not self._conf:
            _conf = [s for s in STEPS if s.get("name") == self.name][0]
            assert _conf is not None
            self._conf = cast(dict[str, str], _conf)

        return self._conf

    @property
    def options(self) -> dict:
        if not self._options:
            o = self.conf.get("options")
            assert o is not None
            self._options = cast(dict[str, str], o)

        return self._options

    def drop(self):
        DEFAULT_LOGGER.warning("💣 (drop)", extra={"step": self})

        fs = self.database.storage
        assert fs

        tmp = fs.join("tmp")
        if tmp.exists():
            tmp.rm()

        checkpoint = fs.join("checkpoints")
        if checkpoint.exists():
            checkpoint.rm()

        schema = fs.join("schemas")
        if schema.exists():
            schema.rm()

        for t in ["jobs", "tables", "dependencies", "views"]:
            tbl = Table("fabricks", self.name, t)
            tbl.drop()

        self.database.drop()

    def create(self):
        DEFAULT_LOGGER.info("🌟 (create)", extra={"step": self})

        if not self.runtime.exists():
            DEFAULT_LOGGER.warning(f"{self.name} not found in runtime ({self.runtime})")
        else:
            self.update()

    def update(self, update_dependencies: Optional[bool] = True, progress_bar: Optional[bool] = False):
        if not self.runtime.exists():
            DEFAULT_LOGGER.warning(f"{self.name} not found in runtime ({self.runtime})")

        else:
            if not self.database.exists():
                self.database.create()

            self.update_jobs()
            self.create_jobs()

            if update_dependencies:
                self.update_dependencies(progress_bar=progress_bar)

            self.update_tables()
            self.update_views()

    def get_dependencies(
        self,
        progress_bar: Optional[bool] = False,
        topic: Optional[Union[str, List[str]]] = None,
    ) -> Tuple[Optional[DataFrame], List[str]]:
        DEFAULT_LOGGER.debug("get dependencies", extra={"step": self})

        errors = []

        def _get_dependencies(row: Row):
            job = get_job(step=self.name, job_id=row["job_id"])
            try:
                df = job.get_dependencies()
                return df

            except Exception as e:
                DEFAULT_LOGGER.exception("failed to get dependencies", extra={"job": job})
                errors.append((job, e))

        job_df = self.get_jobs()
        if topic and job_df:
            if isinstance(topic, str):
                topic = [topic]

            where = ", ".join([f"'{t}'" for t in topic])
            DEFAULT_LOGGER.debug(f"where topic in {where}", extra={"step": self})
            job_df = job_df.where(f"topic in ({where})")

        if job_df:
            job_df = job_df.where("not options.type <=> 'manual'")

            DEFAULT_LOGGER.setLevel(logging.CRITICAL)
            dfs = run_in_parallel(_get_dependencies, job_df, workers=16, progress_bar=progress_bar)
            DEFAULT_LOGGER.setLevel(LOGLEVEL)

            return concat_dfs(dfs), errors

        return None, errors

    def get_jobs_iter(self, topic: Optional[str] = None):
        return read_yaml(self.runtime, root="job", prio_file_name=topic)

    def get_jobs(self, topic: Optional[str] = None) -> Optional[DataFrame]:
        DEFAULT_LOGGER.debug("get jobs", extra={"step": self})

        try:
            conf = get_step_conf(self.name)
            schema = get_schema_for_type(conf)

            df = SPARK.createDataFrame(read_yaml(self.runtime, root="job", prio_file_name=topic), schema=schema)  # type: ignore

            df = df.withColumn("job_id", md5(expr("concat(step, '.' ,topic, '_', item)")))

            duplicated_df = df.groupBy("job_id", "step", "topic", "item").count().where("count > 1")
            duplicates = ",".join(f"{row.step}.{row.topic}_{row.item}" for row in duplicated_df.collect())
            assert duplicated_df.isEmpty(), f"duplicated job(s) ({duplicates})"

            return df if not df.isEmpty() else None

        except AssertionError as e:
            DEFAULT_LOGGER.exception("failed to get jobs", extra={"step": self})
            raise e

    def create_jobs(self, retry: Optional[bool] = True) -> List[str]:
        DEFAULT_LOGGER.info("create jobs", extra={"step": self})

        errors = []

        def _create_job(row: Row):
            job = get_job(step=self.name, job_id=row["job_id"])
            try:
                job.create()
            except:  # noqa E722
                DEFAULT_LOGGER.exception("not created", extra={"job": self})
                errors.append(job)

        df = self.get_jobs()
        table_df = self.database.get_tables()
        view_df = self.database.get_views()

        if df:
            if table_df:
                table_df = table_df.withColumn("job_id", expr("md5(table)"))
                df = df.join(table_df, "job_id", how="left_anti")
            if view_df:
                view_df = view_df.withColumn("job_id", expr("md5(view)"))
                df = df.join(view_df, "job_id", how="left_anti")

            DEFAULT_LOGGER.setLevel(logging.CRITICAL)
            run_in_parallel(_create_job, df, workers=16, progress_bar=True)
            DEFAULT_LOGGER.setLevel(LOGLEVEL)

            if errors:
                for e in errors:
                    DEFAULT_LOGGER.error("not created", extra={"job": e})

                if retry:
                    DEFAULT_LOGGER.warning("retry create jobs", extra={"step": self})
                    self.update_tables()
                    self.update_views()

                    return self.create_jobs(retry=False)

                else:
                    DEFAULT_LOGGER.warning("retry failed", extra={"step": self})

        else:
            DEFAULT_LOGGER.debug("no new job", extra={"step": self})

        return errors

    def update_jobs(self, drop: Optional[bool] = False):
        df = self.get_jobs()
        if df:
            DEFAULT_LOGGER.info("update jobs", extra={"step": self})
            if drop:
                SCD1("fabricks", self.name, "jobs").table.drop()

            SCD1("fabricks", self.name, "jobs").delete_missing(df, keys=["job_id"])

        else:
            DEFAULT_LOGGER.debug("no job", extra={"step": self})

    def update_tables(self):
        df = self.database.get_tables()
        if df:
            DEFAULT_LOGGER.info("update tables", extra={"step": self})
            df = df.withColumn("job_id", expr("md5(table)"))
            SCD1("fabricks", self.name, "tables").delete_missing(df, keys=["job_id"])

        else:
            DEFAULT_LOGGER.debug("no table", extra={"step": self})

    def update_views(self):
        df = self.database.get_views()

        DEFAULT_LOGGER.info("update views", extra={"step": self})
        df = df.withColumn("job_id", expr("md5(view)"))
        SCD1("fabricks", self.name, "views").delete_missing(df, keys=["job_id"])

    def update_dependencies(
        self,
        progress_bar: Optional[bool] = False,
        topic: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        df, errors = self.get_dependencies(progress_bar=progress_bar, topic=topic)

        if df:
            DEFAULT_LOGGER.info("update dependencies", extra={"step": self})
            df.cache()

            if topic is None:
                SCD1("fabricks", self.name, "dependencies").delete_missing(df, keys=["dependency_id"])

            else:
                if isinstance(topic, str):
                    topic = [topic]

                where = ", ".join([f"'{t}'" for t in topic])
                DEFAULT_LOGGER.debug(f"where topic in {where}", extra={"step": self})

                SCD1("fabricks", self.name, "dependencies").delete_missing(
                    df,
                    keys=["dependency_id"],
                    update_where=f"topic in ({where})",
                    uuid=True,
                )

        else:
            DEFAULT_LOGGER.debug("no dependency", extra={"step": self})

        return errors

    def register(self, update: Optional[bool] = False, drop: Optional[bool] = False):
        def _register(row: Row):
            job = get_job(step=self.name, topic=row["topic"], item=row["item"])
            job.register()

        if drop:
            SPARK.sql(f"drop database if exists {self.name} cascade ")
            SPARK.sql(f"create database {self.name}")

        if update:
            self.update_jobs()

        df = self.get_jobs()
        if df:
            table_df = self.database.get_tables()
            if table_df:
                df = df.join(table_df, "job_id", how="left_anti")

        if df:
            DEFAULT_LOGGER.setLevel(logging.CRITICAL)
            run_in_parallel(_register, df, workers=16, progress_bar=True)
            DEFAULT_LOGGER.setLevel(LOGLEVEL)

    def __str__(self):
        return self.name
