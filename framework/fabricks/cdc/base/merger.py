from __future__ import annotations

from typing import Optional, Union

from jinja2 import Environment, PackageLoader
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as CDataFrame

from fabricks.cdc.base.processor import Processor
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.utils.sqlglot import fix as fix_sql


class Merger(Processor):
    def get_merge_context(self, src: Union[DataFrame, str], **kwargs) -> dict:
        if isinstance(src, (DataFrame, CDataFrame)):
            format = "dataframe"
            columns = self.get_columns(src, backtick=False)
        elif isinstance(src, str):
            format = "view"
            columns = self.get_columns(f"select * from {src}", backtick=False)
        else:
            raise ValueError(f"{src} not allowed")

        assert "__merge_key" in columns
        assert "__merge_condition" in columns

        keys = kwargs.get("keys")
        if isinstance(keys, str):
            keys = [keys]

        columns = [c for c in columns if c not in ["__merge_condition", "__merge_key"]]
        fields = [c for c in columns if not c.startswith("__")]
        where = kwargs.get("update_where") if self.table.rows > 0 else None
        soft_delete = "__is_deleted" in columns
        has_source = "__source" in columns
        has_key = "__key" in columns
        has_metadata = "__metadata" in columns
        has_hash = "__hash" in columns
        has_timestamp = "__timestamp" in columns
        has_identity = "__identity" in columns

        # 'NoneType' object is not iterable
        if keys:
            keys = [f"`{k}`" for k in keys]
        if columns:
            columns = [f"`{c}`" for c in columns]
        if fields:
            fields = [f"`{c}`" for c in fields]

        assert "__key" or keys, f"{self} - __key or keys not found"

        return {
            "src": src,
            "format": format,
            "tgt": self.table,
            "cdc": self.change_data_capture,
            "columns": columns,
            "fields": fields,
            "soft_delete": soft_delete,
            "has_source": has_source,
            "has_identity": has_identity,
            "has_key": has_key,
            "has_hash": has_hash,
            "keys": keys,
            "has_metadata": has_metadata,
            "has_timestamp": has_timestamp,
            "where": where,
        }

    def get_merge_query(self, src: Union[DataFrame, str], fix: Optional[bool] = True, **kwargs) -> str:
        context = self.get_merge_context(src=src, **kwargs)
        environment = Environment(loader=PackageLoader("fabricks.cdc", "templates"))
        merge = environment.get_template("merge.sql.jinja")

        try:
            sql = merge.render(**context)
        except Exception as e:
            DEFAULT_LOGGER.debug("context", extra={"job": self, "content": context})
            raise e

        if fix:
            try:
                sql = sql.replace("{src}", "src")
                sql = fix_sql(sql)
                sql = sql.replace("`src`", "{src}")
                DEFAULT_LOGGER.debug("merge", extra={"job": self, "sql": sql})

            except Exception as e:
                DEFAULT_LOGGER.exception("could not clean sql query", extra={"job": self, "sql": sql})
                raise e
        else:
            DEFAULT_LOGGER.debug("merge", extra={"job": self, "sql": sql})

        return sql

    def merge(self, src: Union[DataFrame, Table, str], **kwargs):
        if not self.table.exists():
            self.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        if df:
            global_temp_view = f"{self.database}_{'_'.join(self.levels)}__merge"
            view = create_or_replace_global_temp_view(global_temp_view, df, uuid=kwargs.get("uuid", False))

            merge = self.get_merge_query(view, **kwargs)
            self.spark.sql(merge, src=view)
