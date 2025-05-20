import re
from typing import List, Optional, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.types import Row
from typing_extensions import deprecated

from fabricks.cdc.nocdc import NoCDC
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base._types import SchemaDependencies, TGold
from fabricks.core.jobs.base.job import BaseJob
from fabricks.core.udfs import is_registered, register_udf
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.utils.path import Path
from fabricks.utils.sqlglot import get_tables


class Gold(BaseJob):
    def __init__(
        self,
        step: TGold,
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
    _sql_path: Optional[Path] = None
    _schema_drift: Optional[bool] = None

    @classmethod
    def from_job_id(cls, step: str, job_id: str, *, conf: Optional[Union[dict, Row]] = None):
        return cls(step=cast(TGold, step), job_id=job_id)

    @classmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str, *, conf: Optional[Union[dict, Row]] = None):
        return cls(step=cast(TGold, step), topic=topic, item=item)

    @property
    def stream(self) -> bool:
        return False

    @property
    def schema_drift(self) -> bool:
        if not self._schema_drift:
            _schema_drift = self.step_conf.get("options", {}).get("schema_drift", False)
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
        return self.get_sql()

    @deprecated("use sql instead")
    def get_sql(self) -> str:
        return self.paths.runtime.get_sql()

    def get_udfs(self) -> List[str]:
        # udf not allowed in invoke
        if self.mode == "invoke":
            return []
        # udf not allowed in notebook
        elif self.options.job.get("notebook"):
            return []
        # udf not allowed in table
        elif self.options.job.get("table"):
            return []
        else:
            matches = []
            if "udf_" in self.get_sql():
                r = re.compile(r"(?<=udf_)\w*(?=\()")
                matches = re.findall(r, self.get_sql())
                matches = set(matches)
                matches = list(matches)
            return matches

    def register_udfs(self):
        for u in self.get_udfs():
            if not is_registered(u):
                DEFAULT_LOGGER.debug(f"register udf ({u})", extra={"job": self})
                register_udf(udf=u, spark=self.spark)

    def base_transform(self, df: DataFrame) -> DataFrame:
        df = df.transform(self.extend)
        return df

    def get_data(self, stream=False, transform: Optional[bool] = False) -> DataFrame:
        if self.options.job.get_boolean("requirements"):
            import sys

            sys.path.append("/dbfs/mnt/fabricks/site-packages")

        if self.mode == "invoke":
            df = self.spark.createDataFrame([{}])  # type: ignore

        elif self.options.job.get("notebook"):
            from databricks.sdk.runtime import dbutils

            DEFAULT_LOGGER.debug("run notebook", extra={"job": self})
            path = self.paths.runtime.get_notebook_path()
            global_temp_view = dbutils.notebook.run(path, self.timeout, arguments={})  # type: ignore
            df = self.spark.sql(f"select * from global_temp.{global_temp_view}")

        elif self.options.job.get("table"):
            table = self.options.job.get("table")
            df = self.spark.read.table(table)  # type: ignore

        else:
            assert self.get_sql(), "sql not found"
            self.register_udfs()
            df = self.spark.sql(self.get_sql())

        if transform:
            df = self.base_transform(df)
        return df

    def create_or_replace_view(self):
        assert self.mode == "memory", f"{self.mode} not allowed"

        df = self.spark.sql(self.get_sql())
        cdc_options = self.get_cdc_context(df)
        self.cdc.create_or_replace_view(self.get_sql(), **cdc_options)

    def get_dependencies(self) -> DataFrame:
        data = []
        parents = self.options.job.get_list("parents") or []

        if self.mode == "invoke":
            dependencies = []
        elif self.options.job.get("notebook"):
            dependencies = self._get_notebook_dependencies()
        else:
            dependencies = self._get_sql_dependencies()

        dependencies = [d for d in dependencies if d not in parents]
        dependencies = [d.replace("__current", "") for d in dependencies]
        dependencies = list(set(dependencies))

        for d in dependencies:
            data.append(Row(self.job_id, d, "parser"))

        for p in parents:
            data.append(Row(self.job_id, p, "job"))

        if len(dependencies) == 0:
            DEFAULT_LOGGER.debug("no dependency found", extra={"job": self})
            df = self.spark.createDataFrame(data, SchemaDependencies)

        else:
            df = self.spark.createDataFrame(
                data,
                schema=["job_id", "parent", "origin"],
            )  # order of the fields is important !
            df = df.transform(self.add_dependency_details)

        assert df.where("job_id == parent_id").count() == 0, "circular dependency found"
        return df

    def _get_sql_dependencies(self) -> List[str]:
        from fabricks.core.jobs.base._types import Steps

        steps = [str(s) for s in Steps]
        return get_tables(self.sql, allowed_databases=steps)

    def _get_notebook_dependencies(self) -> List[str]:
        import re

        from fabricks.context import CATALOG

        dependencies = []
        df = self.get_data(self.stream)

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
        if "__order_duplicate_by_asc" in df.columns:
            order_duplicate_by = {"__order_duplicate_by_asc": "asc"}
        elif "__order_duplicate_by_desc" in df.columns:
            order_duplicate_by = {"__order_duplicate_by_desc": "desc"}
        else:
            order_duplicate_by = None

        context = {
            "add_metadata": True,
            "soft_delete": True if self.slowly_changing_dimension else None,
            "deduplicate_key": self.options.job.get_boolean("deduplicate", None),
            "deduplicate_hash": True if self.slowly_changing_dimension else None,
            "deduplicate": False,  # assume no duplicate in gold
            "rectify": False,  # assume no reload in gold
            "order_duplicate_by": order_duplicate_by,
            "correct_valid_from": self.options.job.get_boolean("correct_valid_from", True),
        }

        if self.slowly_changing_dimension:
            if "__key" not in df.columns:
                context["add_key"] = True
            if "__hash" not in df.columns:
                context["add_hash"] = True

            if "__operation" not in df.columns:
                context["deduplicate_hash"] = None  # assume no duplicate hash
                if self.mode == "update":
                    context["add_operation"] = "reload"
                    context["rectify"] = True
                else:
                    context["add_operation"] = "upsert"

        if not reload:
            if self.mode == "update" and self.change_data_capture == "scd2":
                context["slice"] = "update"

        if self.mode == "memory":
            context["mode"] = "complete"

        if self.options.job.get_boolean("persist_last_timestamp"):
            if self.change_data_capture == "scd1":
                if "__timestamp" not in df.columns:
                    context["add_timestamp"] = True
            if self.change_data_capture == "scd2":
                if "__valid_from" not in df.columns:
                    context["add_timestamp"] = True

        return context

    def for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs):
        assert self.persist, f"{self.mode} not allowed"

        reload = kwargs.get("reload")
        context = self.get_cdc_context(df=df, reload=reload)

        # if dataframe, reference is passed (BUG)
        name = f"{self.step}_{self.topic}_{self.item}"
        global_temp_view = create_or_replace_global_temp_view(name=name, df=df)
        sql = f"select * from {global_temp_view}"

        check_df = self.spark.sql(sql)
        if check_df.isEmpty():
            DEFAULT_LOGGER.warning("no data", extra={"job": self})
            return

        if reload:
            DEFAULT_LOGGER.warning("force reload", extra={"job": self})
            self.cdc.complete(sql, **context)

        elif self.mode == "update":
            assert not isinstance(self.cdc, NoCDC), "nocdc update not allowed"
            self.cdc.update(sql, **context)

        elif self.mode == "append":
            assert isinstance(self.cdc, NoCDC), f"{self.change_data_capture} append not allowed"
            self.cdc.append(sql, **context)

        elif self.mode == "complete":
            self.cdc.complete(sql, **context)

        else:
            raise ValueError(f"{self.mode} - not allowed")

        self.check_duplicate_key()
        self.check_duplicate_hash()
        self.check_duplicate_identity()

    def for_each_run(self, **kwargs):
        last_version = None
        if self.options.job.get_boolean("persist_last_timestamp"):
            last_version = self.table.get_last_version()

        if self.mode == "invoke":
            schedule = kwargs.get("schedule", None)
            self.invoke(schedule=schedule)
        else:
            super().for_each_run(**kwargs)

        if self.options.job.get_boolean("persist_last_timestamp"):
            self._update_last_timestamp(last_version=last_version)

    def create(self):
        if self.mode == "invoke":
            DEFAULT_LOGGER.info("invoke (no table nor view)", extra={"job": self})
        else:
            super().create()
            if self.options.job.get_boolean("persist_last_timestamp"):
                self._update_last_timestamp(create=True)

    def register(self):
        if self.options.job.get_boolean("persist_last_timestamp"):
            self.cdc_last_timestamp.table.register()

        if self.mode == "invoke":
            DEFAULT_LOGGER.info("invoke (no table nor view)", extra={"job": self})
        else:
            super().register()

    def drop(self):
        if self.options.job.get_boolean("persist_last_timestamp"):
            self.cdc_last_timestamp.drop()

        super().drop()

    def optimize(
        self,
        vacuum: Optional[bool] = True,
        optimize: Optional[bool] = True,
        analyze: Optional[bool] = True,
    ):
        if self.mode == "memory":
            DEFAULT_LOGGER.debug("memory (no optimize)", extra={"job": self})
        else:
            super().optimize()

    @property
    def cdc_last_timestamp(self) -> NoCDC:
        assert self.mode == "update", "persist_last_timestamp only allowed in update"
        assert self.change_data_capture in ["scd1", "scd2"], "persist_last_timestamp only allowed in scd1 or scd2"

        cdc = NoCDC(self.step, self.topic, f"{self.item}__last_timestamp")
        return cdc

    def _update_last_timestamp(self, last_version: Optional[int] = None, create: bool = False):
        df = self.spark.sql(f"select * from {self} limit 1")

        fields = []
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

    def overwrite(self):
        if self.mode == "invoke":
            DEFAULT_LOGGER.debug("invoke (no overwrite)", extra={"job": self})
            return
        elif self.mode == "memory":
            DEFAULT_LOGGER.debug("memory (no overwrite)", extra={"job": self})
            self.create_or_replace_view()
            return

        self.overwrite_schema()
        self.run(reload=True)
