from __future__ import annotations

from typing import Literal, Optional

from jinja2 import Environment, PackageLoader
from pyspark.sql import DataFrame

from fabricks.cdc.mixins._protocol import CdcProtocol
from fabricks.cdc.mixins._types import AllowedSources
from fabricks.context.config import IS_DEBUGMODE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.models.cdc import CdcContext, QueryContext
from fabricks.utils._types import DataFrameLike
from fabricks.utils.sqlglot import fix as fix_sql

_CTES_PIPELINE = ("__base", "__sliced", "__deduplicated_key", "__rectified", "__deduplicated_hash")


def _previous_cte(stage: str, flags: dict[str, bool]) -> str:
    idx = _CTES_PIPELINE.index(stage)
    return next((s for s in reversed(_CTES_PIPELINE[:idx]) if flags.get(s, s == "__base")), "__base")


class ProcessorMixin(CdcProtocol):
    def get_data(self, src: AllowedSources, context: CdcContext) -> DataFrame:
        if isinstance(src, DataFrameLike):
            name = f"{self.qualified_name}__data"
            global_temp_view = create_or_replace_global_temp_view(name, src, uuid=context.uuid, job=self)
            src = f"select * from {global_temp_view}"
        sql = self.get_query(src, fix=True, context=context)
        DEFAULT_LOGGER.debug("exec query", extra={"label": self, "sql": sql})
        return self.spark.sql(sql)

    def get_query_context(
        self, template: Literal["filter", "merger", "query"], src: AllowedSources, **kwargs
    ) -> QueryContext:
        DEFAULT_LOGGER.debug("deduce query context", extra={"label": self})
        if isinstance(src, DataFrameLike):
            format = "dataframe"
        elif isinstance(src, Table):
            format = "table"
        elif isinstance(src, str):
            format = "query"
        else:
            raise ValueError(f"{src} not allowed")
        context = CdcContext.model_validate(kwargs)
        inputs = self.get_columns(src, backtick=False, sort=False)
        fields = [c for c in inputs if not c.startswith("__")]
        keys = context.keys
        mode = context.mode
        if mode == "update":
            tgt = str(self.table)
        elif mode == "append" and "__timestamp" in inputs:
            tgt = str(self.table)
        else:
            tgt = None
        overwrite: list = []
        exclude = list(context.exclude)
        cast = dict(context.cast)
        order_duplicate_by: Optional[list[str]] = (
            [f"{k} {v}" for k, v in context.order_duplicate_by.items()] if context.order_duplicate_by else None
        )
        add_source = context.add_source
        add_calculated_columns = context.add_calculated_columns
        if add_calculated_columns:
            raise ValueError("add_calculated_columns is not yet supported")
        add_operation = context.add_operation
        add_key = context.add_key
        add_hash = context.add_hash
        add_timestamp = context.add_timestamp
        add_last_updated = context.add_last_updated
        add_metadata = context.add_metadata
        has_order_by = None if not order_duplicate_by else True
        # determine which special columns are present or need to be added to the output
        has_operation = add_operation or "__operation" in inputs
        has_metadata = add_metadata or "__metadata" in inputs
        has_source = add_source or "__source" in inputs
        has_timestamp = add_timestamp or "__timestamp" in inputs
        has_key = add_key or "__key" in inputs
        has_hash = add_hash or "__hash" in inputs
        has_identity = "__identity" in inputs
        has_rescued_data = "__rescued_data" in inputs
        has_last_updated = add_last_updated or "__last_updated" in inputs
        soft_delete = context.soft_delete
        delete_missing = context.delete_missing
        slice = context.slice
        rectify = context.rectify
        deduplicate = context.deduplicate
        deduplicate_key = context.deduplicate_key
        deduplicate_hash = context.deduplicate_hash
        correct_valid_from = context.correct_valid_from
        try:
            rows = self.table.rows
            has_rows = rows > 0
        except Exception:
            rows = None
            has_rows = None
        # only needed when comparing to current
        # delete all records in current if there is no new data
        if mode == "update" and delete_missing and self.change_data_capture in ["scd1", "scd2"]:
            has_no_data = not self.has_data(src)
        else:
            has_no_data = None
        # always deduplicate if not set for slowly changing dimensions
        if self.slowly_changing_dimension:
            if deduplicate is None:
                deduplicate = True
        # order duplicates by implies key deduplication
        if order_duplicate_by:
            deduplicate_key = True
        if deduplicate:
            deduplicate_key = True
            deduplicate_hash = True
        # if any deduplication is requested, deduplicate all
        deduplicate = deduplicate or deduplicate_key or deduplicate_hash
        # always rectify if not set
        if self.slowly_changing_dimension:
            if rectify is None:
                rectify = True
        # only correct valid_from on first load
        if self.slowly_changing_dimension and mode == "update":
            correct_valid_from = correct_valid_from and not has_rows
        # override slice for incremental load if timestamp and rows are present
        if slice is None:
            if mode == "update" and has_timestamp and has_rows:
                slice = "update"
        # override slice for full load if update and table is empty
        if slice == "update" and not has_rows:
            slice = None
        # override operation if added and found in df
        if add_operation and "__operation" in inputs:
            overwrite.append("__operation")
        # override timestamp if added and found in df
        if add_timestamp and "__timestamp" in inputs:
            overwrite.append("__timestamp")
        elif "__timestamp" in inputs:
            cast["__timestamp"] = "timestamp"
        # override key if added and found in df (key needed for merge)
        if add_key and "__key" in inputs:
            overwrite.append("__key")
        # override hash if added and found in df (hash needed to identify fake updates)
        if add_hash and "__hash" in inputs:
            overwrite.append("__hash")
        # override __last_updated if added and found in df
        if add_last_updated and "__last_updated" in inputs:
            overwrite.append("__last_updated")
        # override metadata if added and found in df
        if add_metadata and "__metadata" in inputs:
            overwrite.append("__metadata")
        advanced_ctes = ((rectify or deduplicate) and self.slowly_changing_dimension) or self.slowly_changing_dimension
        advanced_deduplication = advanced_ctes and deduplicate
        # add key and hash if not added nor found in df but exclude from output
        # needed for merge
        if mode == "update" or advanced_ctes or deduplicate:
            if not add_key and "__key" not in inputs:
                add_key = True
                exclude.append("__key")
            if not add_hash and "__hash" not in inputs:
                add_hash = True
                exclude.append("__hash")
        # add operation and timestamp if not added nor found in df but exclude from output
        # needed for deduplication and/or rectification
        if advanced_ctes:
            if not add_operation and "__operation" not in inputs:
                add_operation = "upsert"
                exclude.append("__operation")
            if not add_timestamp and "__timestamp" not in inputs:
                add_timestamp = True
                exclude.append("__timestamp")
        if add_key:
            if keys is None:
                keys = list(fields)
            if has_source:
                keys.append("__source")
        hashes = None
        if add_hash:
            hashes = [f for f in fields]
            if "__operation" in inputs or add_operation:
                hashes.append("__operation")
        if self.change_data_capture == "nocdc":
            intermediates = [i for i in inputs]
            outputs = [i for i in inputs]
        else:
            intermediates = [f for f in fields]
            outputs = [f for f in fields]
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
        if self.change_data_capture == "scd2":
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
        # needed for deduplication and/or rectification
        # might need __operation or __source
        if "__key" not in intermediates:
            intermediates.append("__key")
        if "__hash" not in intermediates:
            intermediates.append("__hash")
        outputs = [o for o in outputs if o not in exclude]
        outputs = self.sort_columns(outputs)
        _flags = {
            "__sliced": bool(slice),
            "__deduplicated_key": bool(deduplicate_key),
            "__rectified": bool(rectify),
            "__deduplicated_hash": bool(deduplicate_hash),
        }
        parent_slice = "__base" if slice else None
        parent_deduplicate_key = _previous_cte("__deduplicated_key", _flags) if deduplicate_key else None
        parent_rectify = _previous_cte("__rectified", _flags) if rectify else None
        parent_deduplicate_hash = _previous_cte("__deduplicated_hash", _flags) if deduplicate_hash else None
        parent_cdc = next((s for s in reversed(_CTES_PIPELINE) if _flags.get(s, s == "__base")), "__base")
        parent_final = "__final"
        return QueryContext(
            template=template,
            debugmode=IS_DEBUGMODE,
            src=src,
            format=format,
            tgt=tgt,
            cdc=self.change_data_capture,
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
            filter_where=context.filter_where,
            update_where=context.update_where,
            parent_slice=parent_slice,
            parent_rectify=parent_rectify,
            parent_deduplicate_key=parent_deduplicate_key,
            parent_deduplicate_hash=parent_deduplicate_hash,
            parent_cdc=parent_cdc,
            parent_final=parent_final,
        )

    def fix_sql(self, sql: str) -> str:
        try:
            sql = sql.replace("{src}", "src")
            sql = fix_sql(sql)
            sql = sql.replace("`src`", "{src}")
            DEFAULT_LOGGER.debug("print query", extra={"label": self, "sql": sql, "target": "buffer"})
            return sql
        except Exception as e:
            DEFAULT_LOGGER.exception("fail to fix sql query", extra={"label": self, "sql": sql})
            raise e

    def fix_context(self, context: dict, fix: Optional[bool] = True) -> dict:
        environment = Environment(loader=PackageLoader("fabricks.cdc", "templates"))
        template = environment.get_template("filter.sql.jinja")
        try:
            context["template"] = "filter"
            sql = template.render(**context)
            if fix:
                sql = self.fix_sql(sql)
            else:
                DEFAULT_LOGGER.debug("print query", extra={"label": self, "sql": sql})
        except (Exception, TypeError) as e:
            DEFAULT_LOGGER.exception("fail to render sql query", extra={"label": self, "context": context})
            raise e
        row = self.spark.sql(sql).collect()[0]
        assert row.slices, "no slices found"
        context["slices"] = row.slices
        if context.get("has_source"):
            assert row.sources, "no sources found"
            context["sources"] = row.sources
        return context

    def get_query(self, src: AllowedSources, context: CdcContext, fix: Optional[bool] = True) -> str:
        ctx = self.get_query_context(template="query", src=src, **context.model_dump()).model_dump()
        environment = Environment(loader=PackageLoader("fabricks.cdc", "templates"))
        try:
            if ctx.get("slice"):
                ctx = self.fix_context(ctx, fix=fix)
            template = environment.get_template("query.sql.jinja")
            sql = template.render(**ctx)
            if fix:
                sql = self.fix_sql(sql)
            else:
                DEFAULT_LOGGER.debug("print query", extra={"label": self, "sql": sql})
        except (Exception, TypeError) as e:
            DEFAULT_LOGGER.debug("context", extra={"label": self, "context": ctx})
            DEFAULT_LOGGER.exception("fail to render sql query", extra={"label": self, "context": ctx})
            raise e
        return sql

    def append(self, src: AllowedSources, context: CdcContext):
        if not self.table.registered:
            self.create_table(src, context=context)
        df = self.get_data(src, context=context)
        df = self.reorder_dataframe(df)
        name = f"{self.qualified_name}__append"
        create_or_replace_global_temp_view(name, df, uuid=context.uuid, job=self)
        append = f"insert into table {self.table} by name select * from global_temp.{name}"
        DEFAULT_LOGGER.debug("exec append", extra={"label": self, "sql": append})
        self.spark.sql(append)

    def overwrite(
        self,
        src: AllowedSources,
        context: CdcContext,
        dynamic: Optional[bool] = False,
    ):
        if not self.table.registered:
            self.create_table(src, context=context)
        df = self.get_data(src, context=context)
        df = self.reorder_dataframe(df)
        if not dynamic:
            if context.update_where:
                dynamic = True
        if dynamic:
            self.spark.sql("set spark.sql.sources.partitionOverwriteMode = dynamic")
        name = f"{self.qualified_name}__overwrite"
        create_or_replace_global_temp_view(name, df, uuid=context.uuid, job=self)
        overwrite = f"insert overwrite table {self.table} by name select * from global_temp.{name}"
        DEFAULT_LOGGER.debug("excec overwrite", extra={"label": self, "sql": overwrite})
        self.spark.sql(overwrite)
