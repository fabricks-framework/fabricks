from typing import Optional, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import Row

from fabricks.cdc.nocdc import NoCDC
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base._types import SchemaDependencies, TBronze, TSilver
from fabricks.core.jobs.base.job import BaseJob
from fabricks.core.jobs.bronze import Bronze
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.utils.helpers import concat_dfs
from fabricks.utils.read.read import read
from fabricks.utils.sqlglot import fix as fix_sql


class Silver(BaseJob):
    def __init__(
        self,
        step: TSilver,
        topic: Optional[str] = None,
        item: Optional[str] = None,
        job_id: Optional[str] = None,
        conf: Optional[Union[dict, Row]] = None,
    ):  # type: ignore
        super().__init__(
            "silver",
            step=step,
            topic=topic,
            item=item,
            job_id=job_id,
            conf=conf,
        )

    _parent_step: Optional[TBronze] = None
    _stream: Optional[bool] = None

    @classmethod
    def from_job_id(cls, step: str, job_id: str, *, conf: Optional[Union[dict, Row]] = None):
        return cls(step=cast(TSilver, step), job_id=job_id, conf=conf)

    @classmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str, *, conf: Optional[Union[dict, Row]] = None):
        return cls(step=cast(TSilver, step), topic=topic, item=item, conf=conf)

    @property
    def stream(self) -> bool:
        if not self._stream:
            _stream = self.options.job.get("stream")
            if _stream is None:
                _stream = self.step_conf.get("options", {}).get("stream")
            self._stream = _stream if _stream is not None else True
        return self._stream  # type: ignore

    @property
    def schema_drift(self) -> bool:
        return True

    @property
    def persist(self) -> bool:
        return self.mode in ["update", "append", "latest"]

    @property
    def virtual(self) -> bool:
        return self.mode in ["combine", "memory"]

    @property
    def parent_step(self) -> TBronze:
        if not self._parent_step:
            _parent_step = self.step_conf.get("options", {}).get("parent")
            _parent_step = cast(TBronze, _parent_step)
            assert _parent_step is not None
            self._parent_step = _parent_step
        return self._parent_step

    def base_transform(self, df: DataFrame) -> DataFrame:
        df = df.transform(self.extend)

        if "__metadata" in df.columns:
            df = df.withColumn(
                "__metadata",
                expr(
                    """
                    struct(
                        __metadata.file_path as file_path,
                        __metadata.file_name as file_name,
                        __metadata.file_size as file_size,            
                        __metadata.file_modification_time as file_modification_time,
                        __metadata.inserted as inserted,
                    cast(current_timestamp() as timestamp) as updated
                    )
                    """
                ),
            )
        return df

    def get_data(self, stream: bool = False, transform: Optional[bool] = False) -> DataFrame:
        dep_df = self.get_dependencies()
        assert dep_df, "not dependency found"
        dep_df = dep_df.orderBy("parent_id")
        dependencies = dep_df.count()

        if self.mode == "memory":
            assert dependencies == 1, f"more than 1 dependency not allowed ({dependencies})"

            parent = dep_df.collect()[0].parent
            df = self.spark.sql(f"select * from {parent}")

        elif self.mode == "combine":
            dfs = []
            for row in dep_df.collect():
                df = self.spark.sql(f"select * from {row.parent}")
                dfs.append(df)

            df = concat_dfs(dfs)
            assert df is not None

        else:
            dfs = []

            for row in dep_df.collect():
                try:
                    bronze = Bronze.from_job_id(step=self.parent_step, job_id=row["parent_id"])
                    if bronze.mode in ["memory", "register"]:
                        # data already transformed if bronze is persisted
                        df = bronze.get_data(stream=stream, transform=True)
                    else:
                        df = read(
                            stream=stream,
                            path=bronze.table.deltapath,
                            file_format="delta",
                            metadata=False,
                            spark=self.spark,
                        )

                    if df:
                        if dependencies > 1:
                            assert "__source" in df.columns, "__source not found"
                        dfs.append(df)

                except Exception as e:
                    DEFAULT_LOGGER.exception("could not get dependencies", extra={"job": self})
                    raise e

            df = concat_dfs(dfs)
            assert df is not None

        # transforms
        df = self.filter_where(df)
        df = self.encrypt(df)
        if transform:
            df = self.base_transform(df)

        return df

    def get_dependencies(self, df: Optional[DataFrame] = None) -> DataFrame:
        dependencies = []

        parents = self.options.job.get_list("parents") or []
        if parents:
            for p in parents:
                dependencies.append(Row(self.job_id, p, "job"))

        else:
            p = f"{self.parent_step}.{self.topic}_{self.item}"
            dependencies.append(Row(self.job_id, p, "parser"))

        if len(dependencies) == 0:
            DEFAULT_LOGGER.debug("no dependency found", extra={"job": self})
            df = self.spark.createDataFrame(dependencies, schema=SchemaDependencies)

        else:
            DEFAULT_LOGGER.debug(f"dependencies ({', '.join([row[1] for row in dependencies])})", extra={"job": self})
            df = self.spark.createDataFrame(
                dependencies, schema=["job_id", "parent", "origin"]
            )  # order of the fields is important !
            df = df.transform(self.add_dependency_details)

        return df

    def create_or_replace_view(self):
        assert self.mode in ["memory", "combine"], f"{self.mode} not allowed"

        dep_df = self.get_dependencies()
        assert dep_df, "dependency not found"

        if self.mode == "combine":
            queries = []

            for row in dep_df.collect():
                columns = self.get_data().columns
                df = self.spark.sql(f"select * from {row.parent}")
                cols = [f"`{c}`" if c in df.columns else f"null as `{c}`" for c in columns if c not in ["__source"]]
                source = "__source" if "__source" in df.columns else f"'{row.parent}' as __source"
                query = f"select {', '.join(cols)}, {source} from {row.parent}"
                queries.append(query)

            sql = f"create or replace view {self.qualified_name} as {' union all '.join(queries)}"
            sql = fix_sql(sql)
            DEFAULT_LOGGER.debug("view", extra={"job": self, "sql": sql})
            self.spark.sql(sql)

        else:
            assert dep_df.count() == 1, "only one dependency allowed"

            parent = dep_df.collect()[0].parent
            sql = f"select * from {parent}"
            sql = fix_sql(sql)
            DEFAULT_LOGGER.debug("view", extra={"job": self, "sql": sql})

            df = self.spark.sql(sql)
            cdc_options = self.get_cdc_context(df)
            self.cdc.create_or_replace_view(sql, **cdc_options)

    def create_or_replace_current_view(self):
        from py4j.protocol import Py4JJavaError

        try:
            DEFAULT_LOGGER.debug("create or replace current view", extra={"job": self})

            df = self.spark.sql(f"select * from {self.qualified_name}")

            where_clause = "-- no where clause"
            if "__is_current" in df.columns:
                where_clause = "where __is_current"

            sql = f"""
            create or replace view {self.qualified_name}__current with schema evolution as
            select
              *
            from
              {self.qualified_name}
              {where_clause}
            """
            # sql = fix_sql(sql)
            # DEFAULT_LOGGER.debug("current view", extra={"job": self, "sql": sql})
            self.spark.sql(sql)

        except Py4JJavaError:
            DEFAULT_LOGGER.exception("could not create or replace view", extra={"job": self})

    def overwrite(self):
        self.truncate()
        self.run()

    def overwrite_schema(self, df: Optional[DataFrame] = None):
        DEFAULT_LOGGER.warning("overwrite schema not allowed", extra={"job": self})

    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = None) -> dict:
        # if dataframe, reference is passed (BUG)
        name = f"{self.step}_{self.topic}_{self.item}__check"
        global_temp_view = create_or_replace_global_temp_view(name=name, df=df)

        not_append = not self.mode == "append"
        nocdc = self.change_data_capture == "nocdc"
        order_duplicate_by = self.options.job.get_dict("order_duplicate_by") or {}

        rectify = False
        if not_append and not nocdc:
            if not self.stream and self.mode == "update" and self.table.exists():
                timestamp = "__valid_from" if self.change_data_capture == "scd2" else "__timestamp"
                extra_check = f" and __timestamp > coalesce((select max({timestamp}) from {self}), cast('0001-01-01' as timestamp))"
            else:
                extra_check = "-- no extra check"

            sql = f"""
                select 
                  __operation
                from
                  {global_temp_view}
                where
                  true
                  and __operation == 'reload'
                  {extra_check}
                limit 
                  1
                """
            sql = fix_sql(sql)
            DEFAULT_LOGGER.debug("check", extra={"job": self, "sql": sql})

            check_df = self.spark.sql(sql)
            if not check_df.isEmpty():
                rectify = True
                DEFAULT_LOGGER.debug("rectify enabled", extra={"job": self})

        context = {
            "soft_delete": self.slowly_changing_dimension,
            "deduplicate": self.options.job.get_boolean("deduplicate", not_append),
            "rectify": rectify,
            "order_duplicate_by": order_duplicate_by,
        }

        if self.slowly_changing_dimension:
            if "__key" not in df.columns:
                context["add_key"] = True

        if self.mode == "memory":
            context["mode"] = "complete"
        if self.mode == "latest":
            context["slice"] = "latest"

        if self.change_data_capture == "scd2":
            context["correct_valid_from"] = True

        if nocdc:
            if "__operation" in df.columns:
                context["except"] = ["__operation"]
        if nocdc and self.mode == "memory":
            if "__operation" not in df.columns:
                context["add_operation"] = "upsert"
                context["except"] = ["__operation"]

        if not self.stream and self.mode == "update":
            context["slice"] = "update"

        return context

    def for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs):
        assert self.persist, f"{self.mode} not allowed"

        context = self.get_cdc_context(df)

        # if dataframe, reference is passed (BUG)
        name = f"{self.step}_{self.topic}_{self.item}"
        if batch is not None:
            name = f"{name}__{batch}"
        global_temp_view = create_or_replace_global_temp_view(name=name, df=df)
        sql = f"select * from {global_temp_view}"

        check_df = self.spark.sql(sql)
        if check_df.isEmpty():
            DEFAULT_LOGGER.warning("no data", extra={"job": self})
            return

        if self.mode == "update":
            assert not isinstance(self.cdc, NoCDC)
            self.cdc.update(sql, **context)

        elif self.mode == "append":
            assert isinstance(self.cdc, NoCDC)
            self.cdc.append(sql, **context)

        elif self.mode == "latest":
            assert isinstance(self.cdc, NoCDC)
            check_df = self.spark.sql(
                f"""
                select 
                  __operation
                from
                  {global_temp_view}
                where
                  __operation <> 'reload'
                limit 
                  1
                """
            )
            assert check_df.isEmpty(), f"{check_df.collect()[0][0]} not allowed"
            self.cdc.complete(sql, **context)

        else:
            raise ValueError(f"{self.mode} - not allowed")

    def create(self):
        super().create()
        self.create_or_replace_current_view()

    def register(self):
        super().register()
        self.create_or_replace_current_view()

    def drop(self):
        super().drop()
        DEFAULT_LOGGER.debug("drop current view", extra={"job": self})
        self.spark.sql(f"drop view if exists {self.qualified_name}__current")

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
