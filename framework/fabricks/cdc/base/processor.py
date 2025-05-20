from __future__ import annotations

from typing import Optional, Union

from jinja2 import Environment, PackageLoader
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as CDataFrame

from fabricks.cdc.base.generator import Generator
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.utils.sqlglot import fix as fix_sql


class Processor(Generator):
    def get_data(self, src: Union[DataFrame, Table, str], **kwargs) -> DataFrame:
        if isinstance(src, (DataFrame, CDataFrame)):
            name = f"{self.database}_{'_'.join(self.levels)}__data"
            global_temp_view = create_or_replace_global_temp_view(name, src, uuid=kwargs.get("uuid", False))
            src = f"select * from {global_temp_view}"

        sql = self.get_query(src, fix=True, **kwargs)
        return self.spark.sql(sql)

    def get_query_context(self, src: Union[DataFrame, Table, str], **kwargs) -> dict:
        if isinstance(src, (DataFrame, CDataFrame)):
            format = "dataframe"
        elif isinstance(src, Table):
            format = "table"
        elif isinstance(src, str):
            format = "query"
        else:
            raise ValueError(f"{src} not allowed")

        columns = self.get_columns(src, backtick=False)
        fields = [c for c in columns if not c.startswith("__")]

        keys = kwargs.get("keys", None)
        mode = kwargs.get("mode", "complete")
        tgt = str(self.table) if mode == "update" else None

        order_duplicate_by = kwargs.get("order_duplicate_by", None)
        if order_duplicate_by:
            order_duplicate_by = [f"{key} {value}" for key, value in order_duplicate_by.items()]

        add_source = kwargs.get("add_source", None)
        add_calculated_columns = kwargs.get("add_calculated_columns", [])
        add_operation = kwargs.get("add_operation", None)
        add_key = kwargs.get("add_key", None)
        add_hash = kwargs.get("add_hash", None)
        add_timestamp = kwargs.get("add_timestamp", None)
        add_metadata = kwargs.get("add_metadata", None)

        has_metadata = add_metadata or "__metadata" in columns
        has_source = add_source or "__source" in columns
        has_timestamp = add_timestamp or "__timestamp" in columns
        has_key = add_key or "__key" in columns
        has_hash = add_hash or "__hash" in columns
        has_identity = "__identity" in columns
        has_rescued_data = "__rescued_data" in columns
        has_order_by = None if not order_duplicate_by else True
        try:
            has_rows = self.table.rows > 0
        except Exception:
            has_rows = None

        slice = kwargs.get("slice", None)
        rectify = kwargs.get("rectify", None)
        deduplicate = kwargs.get("deduplicate", None)
        deduplicate_key = kwargs.get("deduplicate_key", None)
        deduplicate_hash = kwargs.get("deduplicate_hash", None)
        soft_delete = kwargs.get("soft_delete", None)
        correct_valid_from = kwargs.get("correct_valid_from", None)

        if slice is None:
            if mode == "update" and has_timestamp and has_rows:
                slice = "update"

        # override slice if update and table is empty
        if slice == "update" and not has_rows:
            slice = None

        if self.slowly_changing_dimension:
            if deduplicate is None:
                deduplicate = True
            if rectify is None:
                rectify = True

        if order_duplicate_by:
            deduplicate_key = True

        if self.slowly_changing_dimension and mode == "update":
            correct_valid_from = correct_valid_from and self.table.rows == 0

        transformed = slice or rectify or deduplicate or deduplicate_key or deduplicate_hash

        if deduplicate:
            deduplicate_key = True
            deduplicate_hash = True

        all_except = kwargs.get("except", []) or []
        all_overwrite = []

        # override operation if provided and found in df
        if add_operation and "__operation" in columns:
            all_overwrite.append("__operation")
        # add operation if not provided and not found in df BUT remove from output
        elif (transformed or self.slowly_changing_dimension) and not add_operation and "__operation" not in columns:
            add_operation = "upsert"
            if self.change_data_capture == "nocdc":
                all_except.append("__operation")

        # override key if provided and found in df
        if add_key and "__key" in columns:
            all_overwrite.append("__key")
        # add key if not provided and not found in df BUT remove from output
        elif (transformed or keys or self.slowly_changing_dimension) and not add_key and "__key" not in columns:
            add_key = True
            all_except.append("__key")

        # override hash if provided and found in df
        if add_hash and "__hash" in columns:
            all_overwrite.append("__hash")
        # add hash if not provided and not found in df BUT remove from output
        elif (transformed or self.slowly_changing_dimension) and not add_hash and "__hash" not in columns:
            add_hash = True
            all_except.append("__hash")

        # override timestamp if provided and found in df
        if add_timestamp and "__timestamp" in columns:
            all_overwrite.append("__timestamp")
        # add timestamp if not provided and not found in df BUT remove from output
        elif (transformed or self.slowly_changing_dimension) and not add_timestamp and "__timestamp" not in columns:
            add_timestamp = True
            all_except.append("__timestamp")

        # override metadata if provided and found in df
        if add_metadata and "__metadata" in columns:
            all_overwrite.append("__metadata")

        parent_slice = None
        if slice:
            parent_slice = "__base"

        parent_deduplicate_key = None
        if deduplicate_key:
            if slice:
                parent_deduplicate_key = "__sliced"
            else:
                parent_deduplicate_key = "__base"

        parent_rectify = None
        if rectify:
            if deduplicate_key:
                parent_rectify = "__deduplicated_key"
            elif slice:
                parent_rectify = "__sliced"
            else:
                parent_rectify = "__base"

        parent_deduplicate_hash = None
        if deduplicate_hash:
            if rectify:
                parent_deduplicate_hash = "__rectified"
            elif deduplicate_key:
                parent_deduplicate_hash = "__deduplicated_key"
            elif slice:
                parent_deduplicate_hash = "__sliced"
            else:
                parent_deduplicate_hash = "__base"

        parent_cdc = None
        if deduplicate_hash:
            parent_cdc = "__deduplicated_hash"
        elif rectify:
            parent_cdc = "__rectified"
        elif deduplicate_key:
            parent_cdc = "__deduplicated_key"
        elif slice:
            parent_cdc = "__sliced"
        else:
            parent_cdc = "__base"

        parent_final = "__final"

        if add_key:
            keys = keys if keys is not None else fields
            if isinstance(keys, str):
                keys = [keys]
            if has_source:
                keys.append("__source")
            keys = [f"cast(`{k}` as string)" for k in keys]

        hashes = None
        if add_hash:
            hashes = [f"cast(`{f}` as string)" for f in fields]
            if "__operation" in columns or add_operation:
                hashes.append("cast(`__operation` <=> 'delete' as string)")

        if fields:
            if has_order_by:
                if "__order_duplicate_by_desc desc" in order_duplicate_by:
                    fields.append("__order_duplicate_by_desc")
                elif "__order_duplicate_by_asc asc" in order_duplicate_by:
                    fields.append("__order_duplicate_by_asc")
            fields = [f"`{f}`" for f in fields]

        if self.change_data_capture == "nocdc":
            __not_allowed_columns = [
                c
                for c in columns
                if c.startswith("__")
                and c not in self.allowed_leading_columns
                and c not in self.allowed_trailing_columns
            ]
            all_except = all_except + __not_allowed_columns

        return {
            "src": src,
            "format": format,
            "tgt": tgt,
            "cdc": self.change_data_capture,
            "mode": mode,
            # fields
            "fields": fields,
            "keys": keys,
            "hashes": hashes,
            # options
            "slice": slice,
            "rectify": rectify,
            "deduplicate": deduplicate,
            # extra
            "deduplicate_key": deduplicate_key,
            "deduplicate_hash": deduplicate_hash,
            # has
            "has_rows": has_rows,
            "has_source": has_source,
            "has_metadata": has_metadata,
            "has_timestamp": has_timestamp,
            "has_identity": has_identity,
            "has_key": has_key,
            "has_hash": has_hash,
            "has_order_by": has_order_by,
            "has_rescued_data": has_rescued_data,
            # default add
            "add_metadata": add_metadata,
            "add_timestamp": add_timestamp,
            "add_key": add_key,
            "add_hash": add_hash,
            # value add
            "add_operation": add_operation,
            "add_source": add_source,
            "add_calculated_columns": add_calculated_columns,
            # extra
            "order_duplicate_by": order_duplicate_by,
            "soft_delete": soft_delete,
            "correct_valid_from": correct_valid_from,
            # except
            "all_except": all_except,
            "all_overwrite": all_overwrite,
            # filter
            "slices": None,
            "sources": None,
            "filter_where": kwargs.get("filter_where"),
            "update_where": kwargs.get("update_where"),
            # parents
            "parent_slice": parent_slice,
            "parent_rectify": parent_rectify,
            "parent_deduplicate_key": parent_deduplicate_key,
            "parent_deduplicate_hash": parent_deduplicate_hash,
            "parent_cdc": parent_cdc,
            "parent_final": parent_final,
        }

    def fix_sql(self, sql: str) -> str:
        try:
            sql = sql.replace("{src}", "src")
            sql = fix_sql(sql)
            sql = sql.replace("`src`", "{src}")
            DEFAULT_LOGGER.debug("query", extra={"job": self, "sql": sql, "target": "buffer"})
            return sql

        except Exception as e:
            DEFAULT_LOGGER.exception("could not fix sql query", extra={"job": self, "sql": sql})
            raise e

    def fix_context(self, context: dict, fix: Optional[bool] = True, **kwargs) -> dict:
        environment = Environment(loader=PackageLoader("fabricks.cdc", "templates"))
        template = environment.get_template("filter.sql.jinja")

        try:
            sql = template.render(**context)
            if fix:
                sql = self.fix_sql(sql)
            else:
                DEFAULT_LOGGER.debug("fix context", extra={"job": self, "sql": sql})

        except Exception as e:
            DEFAULT_LOGGER.exception("could not execute sql query", extra={"job": self, "context": context})
            raise e

        print(sql)
        row = self.spark.sql(sql).collect()[0]
        assert row.slices, "no slices found"

        context["slices"] = row.slices
        if context.get("has_source"):
            assert row.sources, "no sources found"
            context["sources"] = row.sources

        return context

    def get_query(self, src: Union[DataFrame, Table, str], fix: Optional[bool] = True, **kwargs) -> str:
        context = self.get_query_context(src=src, **kwargs)
        environment = Environment(loader=PackageLoader("fabricks.cdc", "templates"))

        if context.get("slice"):
            context = self.fix_context(context, fix=fix, **kwargs)

        template = environment.get_template("query.sql.jinja")
        try:
            sql = template.render(**context)
            if fix:
                sql = self.fix_sql(sql)
            else:
                DEFAULT_LOGGER.debug("query", extra={"job": self, "sql": sql})

        except Exception as e:
            DEFAULT_LOGGER.exception("could not generate sql query", extra={"job": self, "context": context})
            raise e

        return sql

    def append(self, src: Union[DataFrame, Table, str], **kwargs):
        if not self.table.exists():
            self.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        if df:
            df = self.reorder_columns(df)

            name = f"{self.database}_{'_'.join(self.levels)}__append"
            create_or_replace_global_temp_view(name, df, uuid=kwargs.get("uuid", False))

            DEFAULT_LOGGER.debug("append", extra={"job": self})
            df.write.format("delta").mode("append").save(self.table.delta_path.string)

    def overwrite(
        self,
        src: Union[DataFrame, Table, str],
        dynamic: Optional[bool] = False,
        **kwargs,
    ):
        if not self.table.exists():
            self.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        if df:
            df = self.reorder_columns(df)

            if not dynamic:
                if kwargs.get("update_where"):
                    dynamic = True

            if dynamic:
                self.spark.sql("set spark.sql.sources.partitionOverwriteMode=dynamic")

            DEFAULT_LOGGER.info("overwrite", extra={"job": self})

            name = f"{self.database}_{'_'.join(self.levels)}__overwrite"
            create_or_replace_global_temp_view(name, df, uuid=kwargs.get("uuid", False))

            self.spark.sql(f"insert overwrite table {self.table} by name select * from global_temp.{name}")
