from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Literal, Optional

from jinja2 import Environment, PackageLoader
from pyspark.sql import DataFrame

from fabricks.cdc.config import AllowedSources
from fabricks.context.config import IS_DEBUGMODE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.models.cdc import CDCQueryContext
from fabricks.utils._types import DataFrameLike
from fabricks.utils.sqlglot import fix as fix_sql

if TYPE_CHECKING:
    from fabricks.cdc.base import BaseCDC

_ENV = Environment(loader=PackageLoader("fabricks.cdc", "templates"))


class CDCQuery:
    """Owns the query pipeline: context building, SQL rendering, data fetch,
    and non-merge writes (append / overwrite).

    Never mutates table schema — that belongs to CDCDba.
    """

    def __init__(self, cdc: BaseCDC):
        self._cdc = cdc

    def get_data(self, src: AllowedSources, **kwargs) -> DataFrame:
        cdc = self._cdc
        if isinstance(src, DataFrameLike):
            name = f"{cdc.qualified_name}__data"
            global_temp_view = create_or_replace_global_temp_view(name, src, uuid=kwargs.get("uuid", False), job=cdc)
            src = f"select * from {global_temp_view}"

        sql = self.get_query(src, fix=True, **kwargs)
        DEFAULT_LOGGER.debug("exec query", extra={"label": cdc, "sql": sql})
        return cdc.spark.sql(sql)

    def get_query_context(
        self, template: Literal["filter", "merger", "query"], src: AllowedSources, **kwargs
    ) -> CDCQueryContext:
        cdc = self._cdc
        DEFAULT_LOGGER.debug("deduce query context", extra={"label": cdc})

        if isinstance(src, DataFrameLike):
            format = "dataframe"
        elif isinstance(src, Table):
            format = "table"
        elif isinstance(src, str):
            format = "query"
        else:
            raise ValueError(f"{src} not allowed")

        inputs = cdc.get_columns(src, backtick=False, sort=False)
        fields = [c for c in inputs if not c.startswith("__")]
        keys = kwargs.get("keys", None)

        mode = kwargs.get("mode", "complete")
        if mode == "update":
            tgt = str(cdc.table)
        elif mode == "append" and "__timestamp" in inputs:
            tgt = str(cdc.table)
        else:
            tgt = None

        overwrite = []
        exclude = kwargs.get("exclude", [])
        cast = kwargs.get("cast", {})

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
        add_last_updated = kwargs.get("add_last_updated", None)
        add_metadata = kwargs.get("add_metadata", None)

        has_order_by = None if not order_duplicate_by else True

        has_operation = add_operation or "__operation" in inputs
        has_metadata = add_metadata or "__metadata" in inputs
        has_source = add_source or "__source" in inputs
        has_timestamp = add_timestamp or "__timestamp" in inputs
        has_key = add_key or "__key" in inputs
        has_hash = add_hash or "__hash" in inputs
        has_identity = "__identity" in inputs
        has_rescued_data = "__rescued_data" in inputs
        has_last_updated = add_last_updated or "__last_updated" in inputs

        soft_delete = kwargs.get("soft_delete", None)
        delete_missing = kwargs.get("delete_missing", None)
        slice = kwargs.get("slice", None)
        rectify = kwargs.get("rectify", None)
        deduplicate = kwargs.get("deduplicate", None)
        deduplicate_key = kwargs.get("deduplicate_key", None)
        deduplicate_hash = kwargs.get("deduplicate_hash", None)
        correct_valid_from = kwargs.get("correct_valid_from", None)

        try:
            rows = cdc.table.rows
            has_rows = rows > 0
        except Exception:
            rows = None
            has_rows = None

        if mode == "update" and delete_missing and cdc.change_data_capture in ["scd1", "scd2"]:
            has_no_data = not cdc.has_data(src)
        else:
            has_no_data = None

        if cdc.slowly_changing_dimension:
            if deduplicate is None:
                deduplicate = True

        if order_duplicate_by:
            deduplicate_key = True

        if deduplicate:
            deduplicate_key = True
            deduplicate_hash = True

        deduplicate = deduplicate or deduplicate_key or deduplicate_hash

        if cdc.slowly_changing_dimension:
            if rectify is None:
                rectify = True

        if cdc.slowly_changing_dimension and mode == "update":
            correct_valid_from = correct_valid_from and not has_rows

        if slice is None:
            if mode == "update" and has_timestamp and has_rows:
                slice = "update"

        if slice == "update" and not has_rows:
            slice = None

        if add_operation and "__operation" in inputs:
            overwrite.append("__operation")

        if add_timestamp and "__timestamp" in inputs:
            overwrite.append("__timestamp")
        elif "__timestamp" in inputs:
            cast["__timestamp"] = "timestamp"

        if add_key and "__key" in inputs:
            overwrite.append("__key")

        if add_hash and "__hash" in inputs:
            overwrite.append("__hash")

        if add_last_updated and "__last_updated" in inputs:
            overwrite.append("__last_updated")

        if add_metadata and "__metadata" in inputs:
            overwrite.append("__metadata")

        advanced_ctes = ((rectify or deduplicate) and cdc.slowly_changing_dimension) or cdc.slowly_changing_dimension
        advanced_deduplication = advanced_ctes and deduplicate

        if mode == "update" or advanced_ctes or deduplicate:
            if not add_key and "__key" not in inputs:
                add_key = True
                exclude.append("__key")

            if not add_hash and "__hash" not in inputs:
                add_hash = True
                exclude.append("__hash")

        if advanced_ctes:
            if not add_operation and "__operation" not in inputs:
                add_operation = "upsert"
                exclude.append("__operation")

            if not add_timestamp and "__timestamp" not in inputs:
                add_timestamp = True
                exclude.append("__timestamp")

        if add_key:
            keys = keys if keys is not None else list(fields)
            if isinstance(keys, str):
                keys = [keys]
            if has_source:
                keys.append("__source")

        hashes = None
        if add_hash:
            hashes = list(fields)
            if "__operation" in inputs or add_operation:
                hashes.append("__operation")

        if cdc.change_data_capture == "nocdc":
            intermediates = list(inputs)
            outputs = list(inputs)
        else:
            intermediates = list(fields)
            outputs = list(fields)

        if has_operation:
            if "__operation" not in outputs:
                outputs.append("__operation")
        if has_timestamp:
            if "__timestamp" not in outputs:
                outputs.append("__timestamp")
        if has_key:
            if "__key" not in outputs:
                outputs.append("__key")
        if has_hash:
            if "__hash" not in outputs:
                outputs.append("__hash")

        if has_metadata:
            if "__metadata" not in outputs:
                outputs.append("__metadata")
            if "__metadata" not in intermediates:
                intermediates.append("__metadata")
        if has_last_updated:
            if "__last_updated" not in outputs:
                outputs.append("__last_updated")
            if "__last_updated" not in intermediates:
                intermediates.append("__last_updated")
        if has_source:
            if "__source" not in outputs:
                outputs.append("__source")
            if "__source" not in intermediates:
                intermediates.append("__source")
        if has_identity:
            if "__identity" not in outputs:
                outputs.append("__identity")
            if "__identity" not in intermediates:
                intermediates.append("__identity")
        if has_rescued_data:
            if "__rescued_data" not in outputs:
                outputs.append("__rescued_data")
            if "__rescued_data" not in intermediates:
                intermediates.append("__rescued_data")

        if soft_delete:
            if "__is_deleted" not in outputs:
                outputs.append("__is_deleted")
            if "__is_current" not in outputs:
                outputs.append("__is_current")

        if cdc.change_data_capture == "scd2":
            if "__valid_from" not in outputs:
                outputs.append("__valid_from")
            if "__valid_to" not in outputs:
                outputs.append("__valid_to")
            if "__is_current" not in outputs:
                outputs.append("__is_current")

        if advanced_ctes:
            if "__operation" not in intermediates:
                intermediates.append("__operation")
            if "__timestamp" not in intermediates:
                intermediates.append("__timestamp")

        if "__key" not in intermediates:
            intermediates.append("__key")
        if "__hash" not in intermediates:
            intermediates.append("__hash")

        outputs = [o for o in outputs if o not in exclude]
        outputs = cdc.sort_columns(outputs)

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

        return CDCQueryContext(
            template=template,
            debugmode=IS_DEBUGMODE,
            src=src,
            format=format,
            tgt=tgt,
            cdc=cdc.change_data_capture,
            mode=mode,
            inputs=inputs,
            intermediates=intermediates,
            outputs=outputs,
            fields=fields,
            keys=keys,
            hashes=hashes,
            delete_missing=delete_missing,
            advanced_deduplication=advanced_deduplication,
            slice=slice,
            rectify=rectify,
            deduplicate=deduplicate,
            deduplicate_key=deduplicate_key,
            deduplicate_hash=deduplicate_hash,
            has_no_data=has_no_data,
            has_rows=has_rows,
            has_source=has_source,
            has_metadata=has_metadata,
            has_last_updated=has_last_updated,
            has_timestamp=has_timestamp,
            has_operation=has_operation,
            has_identity=has_identity,
            has_key=has_key,
            has_hash=has_hash,
            has_order_by=has_order_by,
            has_rescued_data=has_rescued_data,
            add_metadata=add_metadata,
            add_timestamp=add_timestamp,
            add_last_updated=add_last_updated,
            add_key=add_key,
            add_hash=add_hash,
            add_operation=add_operation,
            add_source=add_source,
            add_calculated_columns=add_calculated_columns,
            order_duplicate_by=order_duplicate_by,
            soft_delete=soft_delete,
            correct_valid_from=correct_valid_from,
            overwrite=overwrite,
            cast=cast,
            parent_slice=parent_slice,
            parent_rectify=parent_rectify,
            parent_deduplicate_key=parent_deduplicate_key,
            parent_deduplicate_hash=parent_deduplicate_hash,
            parent_cdc=parent_cdc,
            parent_final=parent_final,
            filter_where=kwargs.get("filter_where"),
            update_where=kwargs.get("update_where"),
        )

    def fix_sql(self, sql: str) -> str:
        cdc = self._cdc
        try:
            sql = sql.replace("{src}", "src")
            sql = fix_sql(sql)
            sql = sql.replace("`src`", "{src}")

            DEFAULT_LOGGER.debug("print query", extra={"label": cdc, "sql": sql, "target": "buffer"})
            return sql

        except Exception as e:
            DEFAULT_LOGGER.exception("fail to fix sql query", extra={"label": cdc, "sql": sql})
            raise e

    def fix_context(self, context: CDCQueryContext, fix: Optional[bool] = True, **kwargs) -> CDCQueryContext:
        cdc = self._cdc

        try:
            sql = _ENV.get_template("filter.sql.jinja").render(**vars(dataclasses.replace(context, template="filter")))
            if fix:
                sql = self.fix_sql(sql)
            else:
                DEFAULT_LOGGER.debug("print query", extra={"label": cdc, "sql": sql})

        except (Exception, TypeError) as e:
            DEFAULT_LOGGER.exception("fail to render sql query", extra={"label": cdc, "context": context})
            raise e

        row = cdc.spark.sql(sql).collect()[0]
        assert row.slices, "no slices found"

        sources = None
        if context.has_source:
            assert row.sources, "no sources found"
            sources = row.sources

        return dataclasses.replace(context, slices=row.slices, sources=sources)

    def get_query(self, src: AllowedSources, fix: Optional[bool] = True, **kwargs) -> str:
        cdc = self._cdc
        context = self.get_query_context(template="query", src=src, **kwargs)

        try:
            if context.slice:
                context = self.fix_context(context, fix=fix, **kwargs)

            template = _ENV.get_template("query.sql.jinja")

            sql = template.render(**vars(context))
            if fix:
                sql = self.fix_sql(sql)
            else:
                DEFAULT_LOGGER.debug("print query", extra={"label": cdc, "sql": sql})

        except (Exception, TypeError) as e:
            DEFAULT_LOGGER.debug("context", extra={"label": cdc, "context": context})
            DEFAULT_LOGGER.exception("fail to render sql query", extra={"label": cdc, "context": context})
            raise e

        return sql

    def append(self, src: AllowedSources, **kwargs):
        cdc = self._cdc
        if not cdc.table.registered:
            cdc.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        df = cdc.reorder_dataframe(df)

        name = f"{cdc.qualified_name}__append"
        create_or_replace_global_temp_view(name, df, uuid=kwargs.get("uuid", False), job=cdc)
        append = f"insert into table {cdc.table} by name select * from global_temp.{name}"

        DEFAULT_LOGGER.debug("exec append", extra={"label": cdc, "sql": append})
        cdc.spark.sql(append)

    def overwrite(self, src: AllowedSources, dynamic: Optional[bool] = False, **kwargs):
        cdc = self._cdc
        if not cdc.table.registered:
            cdc.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        df = cdc.reorder_dataframe(df)

        if not dynamic:
            if kwargs.get("update_where"):
                dynamic = True

        if dynamic:
            cdc.spark.sql("set spark.sql.sources.partitionOverwriteMode = dynamic")

        name = f"{cdc.qualified_name}__overwrite"
        create_or_replace_global_temp_view(name, df, uuid=kwargs.get("uuid", False), job=cdc)
        overwrite = f"insert overwrite table {cdc.table} by name select * from global_temp.{name}"

        DEFAULT_LOGGER.debug("excec overwrite", extra={"label": cdc, "sql": overwrite})
        cdc.spark.sql(overwrite)
