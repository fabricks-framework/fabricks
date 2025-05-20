from typing import Optional, Union, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit, md5
from pyspark.sql.types import Row

from fabricks.cdc.nocdc import NoCDC
from fabricks.context import VARIABLES
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base._types import SchemaDependencies, TBronze
from fabricks.core.jobs.base.job import BaseJob
from fabricks.core.parsers import BaseParser
from fabricks.core.parsers.get_parser import get_parser
from fabricks.core.utils import clean
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.utils.helpers import concat_ws
from fabricks.utils.path import Path
from fabricks.utils.read import read


class Bronze(BaseJob):
    def __init__(
        self,
        step: TBronze,
        topic: Optional[str] = None,
        item: Optional[str] = None,
        job_id: Optional[str] = None,
        conf: Optional[Union[dict, Row]] = None,
    ):  # type: ignore
        super().__init__(
            "bronze",
            step=step,
            topic=topic,
            item=item,
            job_id=job_id,
            conf=conf,
        )

    _parser: Optional[BaseParser] = None

    @property
    def stream(self) -> bool:
        return self.mode not in ["register"]

    @property
    def schema_drift(self) -> bool:
        return True

    @property
    def persist(self) -> bool:
        return self.mode in ["append", "register"]

    @property
    def virtual(self) -> bool:
        return False

    @classmethod
    def from_job_id(cls, step: str, job_id: str, *, conf: Optional[Union[dict, Row]] = None):
        return cls(step=cast(TBronze, step), job_id=job_id, conf=conf)

    @classmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str, *, conf: Optional[Union[dict, Row]] = None):
        return cls(step=cast(TBronze, step), topic=topic, item=item, conf=conf)

    @property
    def data_path(self) -> Path:
        uri = self.options.job.get("uri")
        assert uri is not None, "no uri provided in options"
        path = Path.from_uri(uri, regex=VARIABLES)
        return path

    def get_dependencies(self, df: Optional[DataFrame] = None) -> DataFrame:
        dependencies = []

        parents = self.options.job.get_list("parents")
        if parents:
            for p in parents:
                dependencies.append(Row(self.job_id, p, "job"))

        if len(dependencies) == 0:
            DEFAULT_LOGGER.debug("no dependency found", extra={"job": self})
            df = self.spark.createDataFrame(dependencies, schema=SchemaDependencies)

        else:
            df = self.spark.createDataFrame(
                dependencies, schema=["job_id", "parent", "origin"]
            )  # order of the fields is important !
            df = df.transform(self.add_dependency_details)

        return df

    def register_external_table(self):
        options = self.conf.parser_options  # type: ignore
        if options:
            file_format = options.get("file_format")
        else:
            file_format = "delta"

        DEFAULT_LOGGER.debug(f"register external table ({self.data_path})", extra={"job": self})

        try:
            df = self.spark.sql(f"select * from {file_format}.`{self.data_path}`")
            assert len(df.columns) > 1, "external table must have at least one column"
        except Exception as e:
            DEFAULT_LOGGER.exception("read external table failed", extra={"job": self})
            raise e

        self.spark.sql(
            f"create table if not exists {self.qualified_name} using {file_format} location '{self.data_path}'"
        )

    def drop_external_table(self):
        DEFAULT_LOGGER.debug("drop external table", extra={"job": self})
        self.spark.sql(f"drop table if exists {self.qualified_name}")

    def optimize_external_table(
        self,
        vacuum: Optional[bool] = True,
        analyze: Optional[bool] = True,
    ):
        DEFAULT_LOGGER.debug("optimize external table", extra={"job": self})
        if vacuum:
            from delta import DeltaTable

            dt = DeltaTable.forPath(self.spark, self.data_path.string)
            retention_days = 7
            DEFAULT_LOGGER.debug(f"{self.data_path} - vacuum table (removing files older than {retention_days} days)")
            try:
                self.spark.sql("SET self.spark.databricks.delta.retentionDurationCheck.enabled = False")
                dt.vacuum(retention_days * 24)
            finally:
                self.spark.sql("SET self.spark.databricks.delta.retentionDurationCheck.enabled = True")

        if analyze:
            DEFAULT_LOGGER.debug(f"{self.data_path} - compute delta statistics")
            self.spark.sql(f"analyze table delta.`{self.data_path}` compute delta statistics")

    @property
    def parser(self) -> BaseParser:
        if not self._parser:
            assert self.mode not in ["register"], f"{self.mode} not allowed"

            name = self.options.job.get("parser")
            assert name is not None, "parser not found"

            options = self.conf.parser_options or None  # type: ignore
            p = get_parser(name, options)

            self._parser = p

        return self._parser

    def parse(self, stream: bool = False) -> DataFrame:
        """
        Parses the data based on the specified mode and returns a DataFrame.

        Args:
            stream (bool, optional): Indicates whether the data should be read as a stream. Defaults to False.

        Returns:
            DataFrame: The parsed data as a DataFrame.
        """
        if self.mode == "register":
            if stream:
                df = read(
                    stream=stream,
                    path=self.data_path,
                    file_format="delta",
                    # spark=self.spark, (BUG)
                )
            else:
                df = self.spark.sql(f"select * from {self}")

            # cleaning should done by parser
            df = clean(df)

        else:
            df = self.parser.get_data(
                stream=stream,
                data_path=self.data_path,
                schema_path=self.paths.schema,
                spark=self.spark,
            )

        return df

    def get_data(self, stream: bool = False, transform: Optional[bool] = False) -> Optional[DataFrame]:
        df = self.parse(stream)
        df = self.filter_where(df)
        df = self.encrypt(df)

        if transform:
            df = self.base_transform(df)

        return df

    def add_calculated_columns(self, df: DataFrame) -> DataFrame:
        calculated_columns = self.options.job.get_dict("calculated_columns")

        if calculated_columns:
            for key, value in calculated_columns.items():
                DEFAULT_LOGGER.debug(f"add calculated column ({key} -> {value})", extra={"job": self})
                df = df.withColumn(key, expr(f"{value}"))

        return df

    def add_hash(self, df: DataFrame) -> DataFrame:
        if "__hash" not in df.columns:
            fields = [f"`{c}`" for c in df.columns if not c.startswith("__")]
            DEFAULT_LOGGER.debug("add hash", extra={"job": self})

            if "__operation" in df.columns:
                fields += ["__operation == 'delete'"]

            if "__source" in df.columns:
                fields += ["__source"]

            df = df.withColumn("__hash", md5(expr(f"{concat_ws(fields)}")))

        return df

    def add_key(self, df: DataFrame) -> DataFrame:
        if "__key" not in df.columns:
            fields = self.options.job.get_list("keys")
            if fields:
                DEFAULT_LOGGER.debug(f"add key ({', '.join(fields)})", extra={"job": self})

                if "__source" in df.columns:
                    fields = fields + ["__source"]

                fields = [f"`{f}`" for f in fields]
                df = df.withColumn("__key", md5(expr(f"{concat_ws(fields)}")))

        return df

    def add_source(self, df: DataFrame) -> DataFrame:
        if "__source" not in df.columns:
            source = self.options.job.get("source")
            if source:
                DEFAULT_LOGGER.debug(f"add source ({source})", extra={"job": self})
                df = df.withColumn("__source", lit(source))

        return df

    def add_operation(self, df: DataFrame) -> DataFrame:
        if "__operation" not in df.columns:
            operation = self.options.job.get("operation")
            if operation:
                DEFAULT_LOGGER.debug(f"add operation ({operation})", extra={"job": self})
                df = df.withColumn("__operation", lit(operation))

            else:
                df = df.withColumn("__operation", lit("upsert"))

        return df

    def base_transform(self, df: DataFrame) -> DataFrame:
        df = df.transform(self.extend)
        df = df.transform(self.add_calculated_columns)
        df = df.transform(self.add_hash)
        df = df.transform(self.add_operation)
        df = df.transform(self.add_source)
        df = df.transform(self.add_key)

        if "__metadata" in df.columns:
            if self.mode == "register":
                #  https://github.com/delta-io/delta/issues/2014 (BUG)
                df = df.withColumn(
                    "__metadata",
                    expr(
                        f"""
                        struct(
                            concat_ws('/', '{self.data_path}', __timestamp, __operation) as file_path,
                            __metadata.file_name as file_name,
                            __metadata.file_size as file_size,            
                            __metadata.file_modification_time as file_modification_time,
                            cast(current_date() as timestamp) as inserted
                        )
                        """
                    ),
                )

            else:
                df = df.withColumn(
                    "__metadata",
                    expr(
                        """
                        struct(
                            __metadata.file_path as file_path,
                            __metadata.file_name as file_name,
                            __metadata.file_size as file_size,            
                            __metadata.file_modification_time as file_modification_time,
                            cast(current_date() as timestamp) as inserted
                        )
                        """
                    ),
                )

        return df

    def create_or_replace_view(self):
        DEFAULT_LOGGER.warning("create or replace view not allowed", extra={"job": self})

    def overwrite_schema(self, df: Optional[DataFrame] = None):
        DEFAULT_LOGGER.warning("schema overwrite not allowed", extra={"job": self})

    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = None) -> dict:
        return {}

    def for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs):
        assert self.persist, f"{self.mode} not allowed"

        context = self.get_cdc_context(df)

        # if dataframe, reference is passed (BUG)
        name = f"{self.step}_{self.topic}_{self.item}__{batch}"
        global_temp_view = create_or_replace_global_temp_view(name=name, df=df)
        sql = f"select * from {global_temp_view}"

        check_df = self.spark.sql(sql)
        if check_df.isEmpty():
            DEFAULT_LOGGER.warning("no data", extra={"job": self})
            return

        assert isinstance(self.cdc, NoCDC)
        if self.mode == "append":
            self.cdc.append(sql, **context)

    def for_each_run(self, **kwargs):
        if self.mode == "register":
            DEFAULT_LOGGER.info("register (no run)", extra={"job": self})
        elif self.mode == "memory":
            DEFAULT_LOGGER.info("memory (no run)", extra={"job": self})
        else:
            super().for_each_run(**kwargs)

    def create(self):
        if self.mode == "register":
            self.register_external_table()
        elif self.mode == "memory":
            DEFAULT_LOGGER.info("memory (no table nor view)", extra={"job": self})
        else:
            super().create()

    def register(self):
        if self.mode == "register":
            self.register_external_table()
        elif self.mode == "memory":
            DEFAULT_LOGGER.info("memory (no table nor view)", extra={"job": self})
        else:
            super().register()

    def truncate(self):
        if self.mode == "register":
            DEFAULT_LOGGER.info("register (no truncate)", extra={"job": self})
        else:
            super().truncate()

    def restore(self, last_version: Optional[str] = None, last_batch: Optional[str] = None):
        if self.mode == "register":
            DEFAULT_LOGGER.info("register (no restore)", extra={"job": self})
        else:
            super().restore()

    def drop(self):
        if self.mode == "register":
            self.drop_external_table()
        super().drop()

    def optimize(
        self,
        vacuum: Optional[bool] = True,
        optimize: Optional[bool] = True,
        analyze: Optional[bool] = True,
    ):
        if self.mode == "memory":
            DEFAULT_LOGGER.info("memory (no optimize)", extra={"job": self})
        elif self.mode == "register":
            self.optimize_external_table(vacuum, analyze)
        else:
            super().optimize()

    def overwrite(self):
        self.truncate()
        self.run()
