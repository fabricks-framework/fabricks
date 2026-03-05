from collections.abc import Sequence
from typing import List, Literal, Optional, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import Row
from typing_extensions import deprecated

from fabricks.cdc.nocdc import NoCDC
from fabricks.cdc.scd0 import SCD0
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.job import BaseJob
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.models import JobDependency, JobGoldOptions, StepGoldConf, StepGoldOptions
from fabricks.utils.path import GitPath
from fabricks.utils.sqlglot import fix, get_tables, parse_script


class Gold(BaseJob):
    def __init__(
        self,
        step: str,
        topic: Optional[str] = None,
        item: Optional[str] = None,
        job_id: Optional[str] = None,
        conf: Optional[Union[dict, Row]] = None,
    ):  # type: ignore
        super().__init__(
            "gold",
            step=step,
            topic=topic,
            item=item,
            job_id=job_id,
            conf=conf,
        )

    _sql: Optional[str] = None
    _sql_path: Optional[GitPath] = None
    _schema_drift: Optional[bool] = None

    @classmethod
    def from_job_id(cls, step: str, job_id: str, *, conf: Optional[Union[dict, Row]] = None):
        return cls(step=step, job_id=job_id)

    @classmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str, *, conf: Optional[Union[dict, Row]] = None):
        return cls(step=step, topic=topic, item=item)

    @property
    def options(self) -> JobGoldOptions:
        """Direct access to typed gold job options."""
        return self.conf.options  # type: ignore

    @property
    def step_conf(self) -> StepGoldConf:
        """Direct access to typed gold step conf."""
        return self.base_step_conf  # type: ignore

    @property
    def step_options(self) -> StepGoldOptions:
        """Direct access to typed gold step options."""
        return self.base_step_conf.options  # type: ignore

    @property
    def stream(self) -> bool:
        return False

    @property
    def schema_drift(self) -> bool:
        if not self._schema_drift:
            _schema_drift = self.step_conf.options.schema_drift or False
            assert _schema_drift is not None
            self._schema_drift = cast(bool, _schema_drift)
        return self._schema_drift

    @property
    def persist(self) -> bool:
        return self.mode in ["update", "append", "complete"]

    @property
    def virtual(self) -> bool:
        return self.mode in ["memory"]

    @property
    def sql(self) -> str:
        sql = self.paths.to_runtime.get_sql()
        return fix(sql, keep_comments=False)

    @deprecated("use sql instead")
    def get_sql(self) -> str:
        return self.sql

    def get_udfs(self) -> Optional[list[str]]:
        udfs = super().get_udfs()

        # udf not allowed in invoke
        if self.mode == "invoke":
            return udfs

        # udf not allowed in notebook
        elif self.options.notebook:
            return udfs

        # udf not allowed in table
        elif self.options.table:
            return udfs

        else:
            matches = self._match_udfs(self.sql) or []
            if udfs:
                matches += udfs

            if len(matches) > 0:
                return list(set(matches))

    def base_transform(self, df: DataFrame) -> DataFrame:
        df = df.transform(self.extend)
        return df

    def get_data(
        self,
        stream: bool = False,
        transform: Optional[bool] = False,
        schema_only: Optional[bool] = False,
        **kwargs,
    ) -> DataFrame:
        if self.options.requirements:
            import sys

            sys.path.append("/dbfs/mnt/fabricks/site-packages")

        if self.mode == "invoke":
            df = self.spark.createDataFrame([{}])  # type: ignore

        elif self.mode == "register":
            file_format = self.options.file_format or "delta"
            uri = self.options.uri
            assert uri is not None, "uri required for register mode"

            df = self.spark.sql(f"select * from {file_format}.`{uri}`")

        elif self.options.notebook:
            invokers = self.invoker_options.run or [] if self.invoker_options else []
            assert len(invokers) <= 1, "at most one invoker allowed when notebook is true"

            path = None
            if invokers:
                from fabricks.context import PATH_RUNTIME

                path = PATH_RUNTIME.joinpath(invokers[0].notebook) if invokers[0].notebook else None

            if path is None:
                path = self.paths.to_runtime

            assert path is not None, "path could not be resolved"

            global_temp_view = self.invoke(path=path, schema_only=schema_only, **kwargs)
            assert global_temp_view is not None, "global_temp_view not found"

            df = self.spark.sql(f"select * from global_temp.{global_temp_view}")

        elif self.options.table:
            table = self.options.table
            df = self.spark.read.table(table)  # type: ignore

        else:
            assert self.sql, "sql not found"
            self.register_udfs()

            if self.options.script:
                parts = parse_script(self.sql)
                if parts:
                    for p in parts:
                        df = self.spark.sql(p)
                else:
                    df = self.spark.sql(self.sql)

            else:
                df = self.spark.sql(self.sql)

        if transform:
            df = self.base_transform(df)

        if schema_only:
            df = df.where("1 == 2")

        return df

    def create_or_replace_view(self):
        assert self.mode == "memory", f"{self.mode} not allowed"

        df = self.spark.sql(self.sql)
        cdc_options = self.get_cdc_context(df)
        self.cdc.create_or_replace_view(self.sql, **cdc_options)

    def get_dependencies(self) -> Sequence[JobDependency]:
        data = []

        parents = self.options.parents or []
        if parents:
            for p in parents:
                data.append(JobDependency.from_parts(self.job_id, p, "parent"))

        else:
            if self.mode == "invoke":
                dependencies = []
            elif self.options.notebook:
                dependencies = self._get_notebook_dependencies()
            else:
                dependencies = self._get_sql_dependencies()

            dependencies = [d for d in dependencies if d not in parents]
            dependencies = [d.replace("__current", "") for d in dependencies]
            dependencies = list(set(dependencies))

            for d in dependencies:
                data.append(JobDependency.from_parts(self.job_id, d, "parser"))

        wait_for = self.options.wait_for or []
        if wait_for:
            for w in wait_for:
                data.append(JobDependency.from_parts(self.job_id, w, "wait_for"))

        return data

    def _get_sql_dependencies(self) -> List[str]:
        from fabricks.context import Steps

        steps = [str(s) for s in Steps]
        return get_tables(self.sql, allowed_databases=steps)

    def _get_notebook_dependencies(self) -> List[str]:
        import re

        from fabricks.context import CATALOG

        dependencies = []
        df = self.get_data(stream=self.stream)

        if df is not None:
            explain_plan = self.spark.sql("explain extended select * from {df}", df=df).collect()[0][0]

            if CATALOG is None:
                r = re.compile(r"(?<=SubqueryAlias spark_catalog\.)[^.]*\.[^.\n]*")
            else:
                r = re.compile(rf"(?:(?<=SubqueryAlias spark_catalog\.)|(?<=SubqueryAlias {CATALOG}\.))[^.]*\.[^.\n]*")

            matches = re.findall(r, explain_plan)
            dependencies = list(set(matches))

        return dependencies

    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = None) -> dict:
        # assume no duplicate in gold (to improve performance)
        deduplicate = self.options.deduplicate
        # assume no reload in gold (to improve performance)
        rectify = self.options.rectify_as_upserts
        # assume no soft delete in gold for scd1 and scd2 (unless specified otherwise)
        if self.options.hard_delete is not None:
            soft_delete = not self.options.hard_delete
        else:
            soft_delete = True if self.change_data_capture in ["scd1", "scd2"] else None

        add_metadata = self.options.metadata
        if add_metadata is None:
            add_metadata = self.step_conf.options.metadata or False

        context = {
            "add_metadata": add_metadata,
            "soft_delete": soft_delete,
            "deduplicate_key": None,
            "deduplicate_hash": True if self.slowly_changing_dimension else None,
            "deduplicate": False,
            "rectify": False,
        }

        # force deduplicate
        if deduplicate is not None:
            context["deduplicate"] = deduplicate
            context["deduplicate_key"] = deduplicate
            context["deduplicate_hash"] = deduplicate

        # force rectify
        if rectify is not None:
            context["rectify"] = rectify

        # add key and hash when needed
        if self.mode == "update" and self.change_data_capture == "nocdc":
            if "__key" not in df.columns:
                context["add_key"] = True
            if "__hash" not in df.columns:
                context["add_hash"] = True

        # add key and hash when needed
        if self.slowly_changing_dimension:
            if "__key" not in df.columns:
                context["add_key"] = True
            if "__hash" not in df.columns:
                context["add_hash"] = True

        if self.slowly_changing_dimension:
            if "__operation" not in df.columns:
                # assume no duplicate hash
                if deduplicate is None:
                    context["deduplicate_hash"] = None

                if self.mode == "update":
                    context["add_operation"] = "reload"
                    if rectify is None:
                        context["rectify"] = True

                else:
                    context["add_operation"] = "upsert"

        # filter to get latest data
        if not reload:
            if self.mode == "update" and self.change_data_capture == "scd2":
                context["slice"] = "update"

            if self.mode == "update" and self.change_data_capture == "nocdc" and "__timestamp" in df.columns:
                context["slice"] = "update"

            if self.mode == "append" and "__timestamp" in df.columns:
                context["slice"] = "update"

        if self.mode == "memory":
            context["mode"] = "complete"

        # correct __valid_from
        if self.change_data_capture == "scd2":
            context["correct_valid_from"] = (
                self.options.correct_valid_from if self.options.correct_valid_from is not None else True
            )

        # add __timestamp
        if self.options.persist_last_timestamp:
            if self.change_data_capture == "scd1":
                if "__timestamp" not in df.columns:
                    context["add_timestamp"] = True
            if self.change_data_capture == "scd2":
                if "__valid_from" not in df.columns:
                    context["add_timestamp"] = True

        # add __updated
        if self.options.persist_last_updated_timestamp:
            if "__last_updated" not in df.columns:
                context["add_last_updated"] = True
        if self.options.last_updated:
            if "__last_updated" not in df.columns:
                context["add_last_updated"] = True

        if "__order_duplicate_by_asc" in df.columns:
            context["order_duplicate_by"] = {"__order_duplicate_by_asc": "asc"}
        elif "__order_duplicate_by_desc" in df.columns:
            context["order_duplicate_by"] = {"__order_duplicate_by_desc": "desc"}

        return context

    def for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs):
        assert self.persist, f"{self.mode} not allowed"

        reload = kwargs.get("reload")
        context = self.get_cdc_context(df=df, reload=reload)

        # if dataframe, reference is passed (BUG)
        name = f"{self.step}_{self.topic}_{self.item}"
        global_temp_view = create_or_replace_global_temp_view(name=name, df=df, job=self)
        sql = f"select * from {global_temp_view}"

        check_df = self.spark.sql(sql)
        if check_df.isEmpty():
            DEFAULT_LOGGER.warning("no data", extra={"label": self})
            return

        if reload:
            DEFAULT_LOGGER.warning("force reload", extra={"label": self})
            self.cdc.complete(sql, **context)

        elif self.mode == "update":
            self.cdc.update(sql, **context)

        elif self.mode == "append":
            assert isinstance(self.cdc, NoCDC), f"{self.change_data_capture} append not allowed"
            self.cdc.append(sql, **context)

        elif self.mode == "complete":
            assert not isinstance(self.cdc, SCD0), "SCD0 complete not allowed"
            self.cdc.complete(sql, **context)

        else:
            raise ValueError(f"{self.mode} - not allowed")

        self.check_duplicate_key()
        self.check_duplicate_hash()
        self.check_duplicate_identity()

    def for_each_run(self, **kwargs):
        if self.mode == "invoke":
            schedule = kwargs.get("schedule", None)
            self.invoke(schedule=schedule)

        elif self.mode == "register":
            DEFAULT_LOGGER.debug("register (no run)", extra={"label": self})

        else:
            last_version = None

            if self.options.persist_last_timestamp:
                last_version = self.table.get_last_version()
            if self.options.persist_last_updated_timestamp:
                last_version = self.table.get_last_version()
            if self.updater_options and self.updater_options.columns:
                last_version = self.table.get_last_version()

            super().for_each_run(**kwargs)

            if self.options.persist_last_timestamp:
                self._persist_timestamp(field="__timestamp", last_version=last_version)

            if self.options.persist_last_updated_timestamp:
                self._persist_timestamp(field="__last_updated", last_version=last_version)

            if self.updater_options and self.updater_options.columns:
                self._update_post_run(last_version=last_version)

    def create(self):
        if self.mode == "invoke":
            DEFAULT_LOGGER.info("invoke (no table nor view)", extra={"label": self})

        elif self.mode == "register":
            self.register_external_table()

        else:
            self.register_udfs()
            super().create()

            if self.options.persist_last_timestamp:
                self._persist_timestamp(create=True)

            if self.updater_options and self.updater_options.columns:
                self._update__columns(drop=False)

    def register_external_table(self):
        DEFAULT_LOGGER.info("register", extra={"label": self})

        file_format = self.options.file_format or "delta"
        uri = self.options.uri
        assert uri is not None, "uri required for register mode"

        self._register_external_table(file_format=file_format, uri=uri)

    def register(self):
        if self.options.persist_last_timestamp:
            self.cdc_last_timestamp.table.register()

        if self.mode == "invoke":
            DEFAULT_LOGGER.info("invoke (no table nor view)", extra={"label": self})

        elif self.mode == "register":
            self.register_external_table()

        else:
            super().register()

    def drop(self):
        if self.options.persist_last_timestamp:
            self.cdc_last_timestamp.drop()

        if self.mode == "register":
            self._drop_external_table()

        super().drop()

    @property
    def cdc_last_timestamp(self) -> NoCDC:
        assert self.mode == "update", "persist_last_timestamp only allowed in update"
        assert self.change_data_capture in ["scd1", "scd2"], "persist_last_timestamp only allowed in scd1 or scd2"

        cdc = NoCDC(self.step, self.topic, f"{self.item}__last_timestamp")
        return cdc

    def _persist_timestamp(
        self,
        field: Literal["__timestamp", "__last_updated"] = "__timestamp",
        last_version: Optional[int] = None,
        create: bool = False,
    ):
        df = self.spark.sql(f"select * from {self} limit 1")

        fields = []

        if field == "__last_updated":
            fields.append("max(__last_updated) :: timestamp as __last_updated")

        elif field == "__timestamp":
            if self.change_data_capture == "scd1":
                fields.append("max(__timestamp) :: timestamp as __timestamp")
            elif self.change_data_capture == "scd2":
                fields.append("max(__valid_from) :: timestamp as __timestamp")

        if "__source" in df.columns:
            fields.append("__source")

        asof = None
        if last_version is not None:
            asof = f"version as of {last_version}"

        sql = f"select {', '.join(fields)} from {self} {asof} group by all"
        df = self.spark.sql(sql)

        if create:
            self.cdc_last_timestamp.table.create(df)
        else:
            self.cdc_last_timestamp.overwrite(df)

    def overwrite(self, schedule: Optional[str] = None, invoke: Optional[bool] = False):
        if self.mode == "invoke":
            DEFAULT_LOGGER.debug("invoke (no overwrite)", extra={"label": self})
            return

        elif self.mode == "memory":
            DEFAULT_LOGGER.debug("memory (no overwrite)", extra={"label": self})
            self.create_or_replace_view()
            return

        self.overwrite_schema()
        self.run(reload=True, schedule=schedule, invoke=invoke)

    def _update_post_run(self, last_version: Optional[int] = None):
        if self.updater_options and self.updater_options.columns:
            DEFAULT_LOGGER.info("update post run", extra={"label": self})

            columns = self.updater_options.columns

            assert self.mode == "update", f"{self.mode} not allowed for update post run"
            assert self.change_data_capture == "scd1", f"{self.change_data_capture} not allowed for update post run"

            assert "__key" in self.table.columns, "__key required for update post run"
            assert "__hash" in self.table.columns, "__hash required for update post run"

            if last_version is not None:
                df_last_version = self.spark.sql(f"select * from {self} version as of {last_version}")
                df_current = self.spark.sql(f"select * from {self}")

                df = self.spark.sql(
                    """
                    select
                        c.*
                    from
                        {df_current} c
                        left anti join {df_last_version} l
                          on c.__key = l.__key
                          and c.__hash = l.__hash
                    """,
                    df_current=df_current,
                    df_last_version=df_last_version,
                )

            else:
                df = self.spark.sql(f"select * from {self}")

            if not df.isEmpty():
                columns = self.updater_options.columns
                for c, expression in columns.items():
                    assert c.startswith("__updated_"), f"{c} not allowed, columns must start with __updated_"
                    df = df.withColumn(c, expr(expression).cast("variant"))

            name = f"{self.step}_{self.topic}_{self.item}"
            global_temp_view = create_or_replace_global_temp_view(name=f"{name}__update_post_run", df=df, job=self)

            merge = f"""
            merge into {self} t using {global_temp_view} s
              on t.__key = s.__key 
              when
                matched then update set {", ".join([f"t.{c} = s.{c}" for c in columns.keys()])}
            """
            merge = fix(merge)

            DEFAULT_LOGGER.debug("exec merge", extra={"label": self, "sql": merge})
            self.spark.sql(merge)

    def _update__columns(self, drop: bool = False):
        if self.updater_options and self.updater_options.columns:
            columns = [c for c in self.updater_options.columns.keys() if c.startswith("__updated_")]

            if drop:
                # drop __columns (from the updater options) that are not in the table anymore
                for c in self.table.columns:
                    if c not in columns and c.startswith("__updated_"):
                        self.table.drop_column(c)

            # add __columns (from the updater options) that are not in the table yet
            for c in columns:
                if c not in self.table.columns:
                    self.table.add_column(c, type="variant")

    def overwrite_schema(self, df=None):
        if not self.mode == "invoke":
            super().overwrite_schema(df)
            self._update__columns(drop=True)

    def update_schema(self, df=None, widen_types=False):
        if not self.mode == "invoke":
            super().update_schema(df, widen_types)
            self._update__columns(drop=False)
