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
            name = f"{self.qualified_name}__data"
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

        inputs = self.get_columns(src, backtick=False, sort=False)
        fields = [c for c in inputs if not c.startswith("__")]

        keys = kwargs.get("keys", None)
        mode = kwargs.get("mode", "complete")
        if mode == "update":
            tgt = str(self.table)
        elif mode == "append" and "__timestamp" in inputs:
            tgt = str(self.table)
        else:
            tgt = None

        order_duplicate_by = kwargs.get("order_duplicate_by", None)
        if order_duplicate_by:
            order_duplicate_by = [f"{key} {value}" for key, value in order_duplicate_by.items()]

        add_source = kwargs.get("add_source", None)
        add_calculated_columns = kwargs.get("add_calculated_columns", [])
        if add_calculated_columns:
            raise ValueError("add_calculated_columns is not yet supported")
        add_operation = kwargs.get("add_operation", None)
        add_key = kwargs.get("add_key", None)
        add_hash = kwargs.get("add_hash", None)
        add_timestamp = kwargs.get("add_timestamp", None)
        add_metadata = kwargs.get("add_metadata", None)

        has_order_by = None if not order_duplicate_by else True
        try:
            has_rows = self.table.rows > 0
        except Exception:
            has_rows = None
        has_data = self.has_data(src)

        # determine which special columns are present or need to be added to the output
        has_operation = add_operation or "__operation" in inputs
        has_metadata = add_metadata or "__metadata" in inputs
        has_source = add_source or "__source" in inputs
        has_timestamp = add_timestamp or "__timestamp" in inputs
        has_key = add_key or "__key" in inputs
        has_hash = add_hash or "__hash" in inputs
        has_identity = "__identity" in inputs
        has_rescued_data = "__rescued_data" in inputs

        soft_delete = kwargs.get("soft_delete", None)
        delete_missing = kwargs.get("delete_missing", None)
        slice = kwargs.get("slice", None)
        rectify = kwargs.get("rectify", None)
        deduplicate = kwargs.get("deduplicate", None)
        deduplicate_key = kwargs.get("deduplicate_key", None)
        deduplicate_hash = kwargs.get("deduplicate_hash", None)
        correct_valid_from = kwargs.get("correct_valid_from", None)

        if slice is None:
            if mode == "update" and has_timestamp and has_rows:
                slice = "update"

        # override slice to full load if update and table is empty
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

        if deduplicate:
            deduplicate_key = True
            deduplicate_hash = True

        overwrite = []
        exclude = kwargs.get("exclude", [])

        # override operation if provided and found in df
        if add_operation and "__operation" in inputs:
            overwrite.append("__operation")

        # override timestamp if provided and found in df
        if add_timestamp and "__timestamp" in inputs:
            overwrite.append("__timestamp")

        # override key if provided and found in df (key needed for merge)
        if add_key and "__key" in inputs:
            overwrite.append("__key")

        # override hash if provided and found in df (hash needed to identify fake updates)
        if add_hash and "__hash" in inputs:
            overwrite.append("__hash")

        # override metadata if provided and found in df
        if add_metadata and "__metadata" in inputs:
            overwrite.append("__metadata")

        advanced_ctes = rectify or deduplicate or deduplicate_key or deduplicate_hash

        if self.slowly_changing_dimension or advanced_ctes:
            # add operation if not provided and not found in df but exclude from output
            if not add_operation and "__operation" not in inputs:
                add_operation = "upsert"
                exclude.append("__operation")

            # add timestamp if not provided and not found in df but exclude from output
            if not add_timestamp and "__timestamp" not in inputs:
                add_timestamp = True
                exclude.append("__timestamp")

            # add key if not provided and not found in df but exclude from output
            if not add_key and "__key" not in inputs:
                add_key = True
                exclude.append("__key")

            # add hash if not provided and not found in df but exclude from output
            if not add_hash and "__hash" not in inputs:
                add_hash = True
                exclude.append("__hash")

        elif mode == "update":
            # add key if not provided and not found in df but exclude from output
            if not add_key and "__key" not in inputs:
                add_key = True
                exclude.append("__key")

            # add hash if not provided and not found in df but exclude from output
            if not add_hash and "__hash" not in inputs:
                add_hash = True
                exclude.append("__hash")

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
            keys = keys if keys is not None else [f for f in fields]
            if isinstance(keys, str):
                keys = [keys]
            if has_source:
                keys.append("__source")

        hashes = None
        if add_hash:
            hashes = [f for f in fields]
            if "__operation" in inputs or add_operation:
                hashes.append("__operation")

        outputs = [f for f in fields]
        intermediates = [f for f in fields]

        if advanced_ctes:
            intermediates += ["__key", "__hash", "__operation", "__timestamp"]

        if has_operation:
            outputs.append("__operation")
        if has_timestamp:
            outputs.append("__timestamp")
        if has_key:
            outputs.append("__key")
        if has_hash:
            outputs.append("__hash")

        if has_metadata:
            outputs.append("__metadata")
            intermediates.append("__metadata")
        if has_source:
            outputs.append("__source")
            intermediates.append("__source")
        if has_identity:
            outputs.append("__identity")
            intermediates.append("__identity")
        if has_rescued_data:
            outputs.append("__rescued_data")
            intermediates.append("__rescued_data")

        if soft_delete:
            outputs += ["__is_current", "__is_deleted"]

        if self.change_data_capture == "scd2":
            outputs += ["__is_current", "__valid_from", "__valid_to"]

        outputs = list(set(outputs))
        outputs = [o for o in outputs if o not in exclude]
        outputs = self.sort_columns(outputs)

        return {
            "src": src,
            "format": format,
            "tgt": tgt,
            "cdc": self.change_data_capture,
            "mode": mode,
            # fields
            "inputs": inputs,
            "intermediates": intermediates,
            "outputs": outputs,
            "fields": fields,
            "keys": keys,
            "hashes": hashes,
            # options
            "delete_missing": delete_missing,
            "slice": slice,
            "rectify": rectify,
            "deduplicate": deduplicate,
            # extra
            "deduplicate_key": deduplicate_key,
            "deduplicate_hash": deduplicate_hash,
            # has
            "has_data": has_data,
            "has_rows": has_rows,
            "has_source": has_source,
            "has_metadata": has_metadata,
            "has_timestamp": has_timestamp,
            "has_operation": has_operation,
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
            # overwrite
            "overwrite": overwrite,
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

        except (Exception, TypeError) as e:
            DEFAULT_LOGGER.exception("could not execute sql query", extra={"job": self, "context": context})
            raise e

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

        try:
            if context.get("slice"):
                context = self.fix_context(context, fix=fix, **kwargs)

            template = environment.get_template("query.sql.jinja")

            sql = template.render(**context)
            if fix:
                sql = self.fix_sql(sql)
            else:
                DEFAULT_LOGGER.debug("query", extra={"job": self, "sql": sql})

        except (Exception, TypeError) as e:
            DEFAULT_LOGGER.debug("context", extra={"job": self, "context": context})
            DEFAULT_LOGGER.exception("could not generate sql query", extra={"job": self, "context": context})
            raise e

        return sql

    def append(self, src: Union[DataFrame, Table, str], **kwargs):
        if not self.table.exists():
            self.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        df = self.reorder_dataframe(df)

        name = f"{self.qualified_name}__append"
        create_or_replace_global_temp_view(name, df, uuid=kwargs.get("uuid", False))

        DEFAULT_LOGGER.debug("append", extra={"job": self})
        self.spark.sql(f"insert into table {self.table} by name select * from global_temp.{name}")

    def overwrite(
        self,
        src: Union[DataFrame, Table, str],
        dynamic: Optional[bool] = False,
        **kwargs,
    ):
        if not self.table.exists():
            self.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        df = self.reorder_dataframe(df)

        if not dynamic:
            if kwargs.get("update_where"):
                dynamic = True

        if dynamic:
            self.spark.sql("set spark.sql.sources.partitionOverwriteMode = dynamic")

        name = f"{self.qualified_name}__overwrite"
        create_or_replace_global_temp_view(name, df, uuid=kwargs.get("uuid", False))

        DEFAULT_LOGGER.debug("overwrite", extra={"job": self})
        self.spark.sql(f"insert overwrite table {self.table} by name select * from global_temp.{name}")
