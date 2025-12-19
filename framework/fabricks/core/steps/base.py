import logging
from typing import Dict, Iterable, List, Literal, Optional, Tuple, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, md5
from pyspark.sql.types import Row
from typing_extensions import deprecated

from fabricks.cdc import NoCDC
from fabricks.context import CONF_RUNTIME, LOGLEVEL, PATHS_RUNTIME, PATHS_STORAGE, SPARK, STEPS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base._types import Bronzes, Golds, SchemaDependencies, Silvers, TStep
from fabricks.core.jobs.get_job import get_job
from fabricks.core.steps._types import Timeouts
from fabricks.core.steps.get_step_conf import get_step_conf
from fabricks.metastore.database import Database
from fabricks.metastore.table import Table
from fabricks.utils.helpers import run_in_parallel
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

    def create(self):
        DEFAULT_LOGGER.info("create", extra={"label": self})

        if not self.runtime.exists():
            DEFAULT_LOGGER.warning(f"could not find {self.name} in runtime")
        else:
            self.update()

    def update(self, update_dependencies: Optional[bool] = True, progress_bar: Optional[bool] = False):
        if not self.runtime.exists():
            DEFAULT_LOGGER.warning(f"could not find {self.name} in runtime")

        else:
            if not self.database.exists():
                self.database.create()

            self.update_configurations()
            errors = self.create_db_objects()

            for e in errors:
                DEFAULT_LOGGER.exception("fail to create db object", extra={"label": e["job"]}, exc_info=e["error"])

            if update_dependencies:
                self.update_dependencies(progress_bar=progress_bar)

            self.update_tables_list()
            self.update_views_list()
            self.update_steps_list()

    def get_dependencies(
        self,
        progress_bar: Optional[bool] = False,
        topic: Optional[Union[str, List[str]]] = None,
        include_manual: Optional[bool] = False,
        loglevel: Optional[Literal[10, 20, 30, 40, 50]] = None,
    ) -> Tuple[DataFrame, List[Dict]]:
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

        errors = [res for res in results if res.get("error")]
        dependencies = []
        for res in [res for res in results if res.get("dependencies")]:
            dependencies.extend(res.get("dependencies"))

        df = self.spark.createDataFrame([d.model_dump() for d in dependencies], SchemaDependencies)  # type: ignore
        return df, errors

    def get_jobs_iter(self, topic: Optional[str] = None) -> Iterable[dict]:
        return read_yaml(self.runtime, root="job", preferred_file_name=topic)

    def get_jobs(self, topic: Optional[str] = None) -> DataFrame:
        DEFAULT_LOGGER.debug("get jobs", extra={"label": self})

        try:
            conf = get_step_conf(self.name)
            schema = get_schema_for_type(conf)
            jobs = self.get_jobs_iter(topic=topic)

            df = SPARK.createDataFrame(jobs, schema=schema)  # type: ignore
            df = df.withColumn("job_id", md5(expr("concat(step, '.' ,topic, '_', item)")))

            duplicated_df = df.groupBy("job_id", "step", "topic", "item").count().where("count > 1")
            duplicates = ",".join(f"{row.step}.{row.topic}_{row.item}" for row in duplicated_df.collect())
            assert duplicated_df.isEmpty(), f"duplicated job(s) ({duplicates})"

            if not df:
                raise ValueError("no jobs found")

            return df

        except AssertionError as e:
            DEFAULT_LOGGER.exception("fail to get jobs", extra={"label": self})
            raise e

    def create_db_objects(
        self,
        retry: Optional[bool] = True,
        update_lists: Optional[bool] = True,
        incremental: Optional[bool] = False,
    ) -> List[Dict]:
        DEFAULT_LOGGER.info("create db objects", extra={"label": self})

        df = self.get_jobs()

        if incremental:
            table_df = self.database.get_tables()
            view_df = self.database.get_views()

            df = df.join(table_df, "job_id", how="left_anti")
            df = df.join(view_df, "job_id", how="left_anti")

        if df:
            results = run_in_parallel(
                _create_db_object,
                df,
                workers=16,
                progress_bar=True,
                logger=DEFAULT_LOGGER,
                loglevel=logging.CRITICAL,
            )

        if update_lists:
            self.update_tables_list()
            self.update_views_list()

        errors = [res for res in results if res.get("error")]

        if errors:
            if retry:
                DEFAULT_LOGGER.warning("retry to create jobs", extra={"label": self})
                return self.create_db_objects(retry=False, update_lists=update_lists, incremental=incremental)

        return errors

    @deprecated("use create_db_objects instead")
    def create_jobs(self, retry: Optional[bool] = True) -> List[Dict]:
        return self.create_db_objects(retry=retry)

    @deprecated("use update_configurations instead")
    def update_jobs(self, drop: Optional[bool] = False):
        return self.update_configurations(drop=drop)

    def update_configurations(self, drop: Optional[bool] = False):
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

    @deprecated("use update_tables_list instead")
    def update_tables(self):
        return self.update_tables_list()

    def update_tables_list(self):
        df = self.database.get_tables()
        df = df.withColumn("job_id", expr("md5(table)"))

        DEFAULT_LOGGER.info("update tables list", extra={"label": self})
        NoCDC("fabricks", self.name, "tables").delete_missing(df, keys=["job_id"])

    @deprecated("use update_views_list instead")
    def update_views(self):
        return self.update_views_list()

    def update_views_list(self):
        df = self.database.get_views()
        df = df.withColumn("job_id", expr("md5(view)"))

        DEFAULT_LOGGER.info("update views list", extra={"label": self})
        NoCDC("fabricks", self.name, "views").delete_missing(df, keys=["job_id"])

    def update_dependencies(
        self,
        progress_bar: Optional[bool] = False,
        topic: Optional[Union[str, List[str]]] = None,
        include_manual: Optional[bool] = False,
        loglevel: Optional[Literal[10, 20, 30, 40, 50]] = None,
    ) -> List[Dict]:
        df, errors = self.get_dependencies(
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
                keys=["dependency_id"],
                update_where=update_where,
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
                keys=["dependency_id"],
                update_where=update_where,
                uuid=True,
            )

        return errors

    def register(self, update: Optional[bool] = False, drop: Optional[bool] = False):
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

    def update_steps_list(self):
        order = self.options.get("order", 0)
        df = SPARK.sql(f"select '{self.expand}' as expand, '{self.name}' as step, '{order}' :: int as `order`")

        NoCDC("fabricks", "steps").delete_missing(df, keys=["step"], update_where=f"step = '{self.name}'")

    def __str__(self):
        return self.name


# to avoid AttributeError: can't pickle local object
def _get_dependencies(row: Row):
    job = get_job(step=row["step"], job_id=row["job_id"])
    try:
        return {"job": str(job), "dependencies": job.get_dependencies()}
    except Exception as e:
        DEFAULT_LOGGER.exception("fail to get dependencies", extra={"label": job})
        return {"job": str(job), "error": e}


def _create_db_object(row: Row):
    job = get_job(step=row["step"], job_id=row["job_id"])
    try:
        job.create()
        return {"job": str(job)}
    except Exception as e:  # noqa E722
        DEFAULT_LOGGER.exception("fail to create db object", extra={"label": job})
        return {"job": str(job), "error": e}


def _register(row: Row):
    job = get_job(step=row["step"], topic=row["topic"], item=row["item"])
    try:
        job.register()
        return {"job": str(job)}
    except Exception as e:
        DEFAULT_LOGGER.exception("fail to get dependencies", extra={"label": job})
        return {"job": str(job), "error": e}
