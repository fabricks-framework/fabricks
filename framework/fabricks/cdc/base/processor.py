from __future__ import annotations

from typing import Any

from jinja2 import Environment, PackageLoader
from pyspark.sql import DataFrame

from fabricks.cdc.base._types import AllowedSources
from fabricks.cdc.base.generator import Generator
from fabricks.context.config import IS_DEBUGMODE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.table import Table
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.utils._types import DataFrameLike
from fabricks.utils.sqlglot import fix as fix_sql


def _get_overwrite(inputs: list[str], *, truncate_as_reload: bool, **add: Any) -> list[str]:  # noqa: ANN401 - add_* values
    overwrite = []
    if add["add_operation"] and "__operation" in inputs:
        overwrite.append("__operation")
    if truncate_as_reload:
        overwrite.append("__operation")
    for name in ["timestamp", "key", "hash", "last_updated", "metadata"]:
        if add[f"add_{name}"] and f"__{name}" in inputs:
            overwrite.append(f"__{name}")
    return overwrite


def _get_parents(
    slice: str | None, rectify: bool | None, deduplicate_key: bool | None, deduplicate_hash: bool | None
) -> dict:
    """Name the CTE each stage reads from, given which stages are enabled."""
    base = "__sliced" if slice else "__base"
    after_key = "__deduplicated_key" if deduplicate_key else base
    after_rectify = "__rectified" if rectify else after_key
    return {
        "parent_slice": "__base" if slice else None,
        "parent_rectify": after_key if rectify else None,
        "parent_deduplicate_key": base if deduplicate_key else None,
        "parent_deduplicate_hash": after_rectify if deduplicate_hash else None,
        "parent_cdc": "__deduplicated_hash" if deduplicate_hash else after_rectify,
        "parent_final": "__final",
    }


def _get_format(src: AllowedSources) -> str:
    if isinstance(src, DataFrameLike):
        return "dataframe"
    if isinstance(src, Table):
        return "table"
    if isinstance(src, str):
        return "query"
    raise ValueError(f"{src} not allowed")


def _resolve_deduplication(
    deduplicate: bool | None,
    deduplicate_key: bool | None,
    deduplicate_hash: bool | None,
    *,
    default: bool,
    ordered: bool,
) -> tuple[bool | None, bool | None, bool | None]:
    # always deduplicate if not set for slowly changing dimensions
    if default and deduplicate is None:
        deduplicate = True
    # order duplicates by implies key deduplication
    if ordered:
        deduplicate_key = True
    if deduplicate:
        deduplicate_key = True
        deduplicate_hash = True
    # if any deduplication is requested, deduplicate all
    return deduplicate or deduplicate_key or deduplicate_hash, deduplicate_key, deduplicate_hash


class Processor(Generator):
    def get_data(self, src: AllowedSources, **kwargs: Any) -> DataFrame:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        if isinstance(src, DataFrameLike):
            name = f"{self.qualified_name}__data"
            global_temp_view = create_or_replace_global_temp_view(name, src, uuid=kwargs.get("uuid", False), job=self)
            src = f"select * from {global_temp_view}"

        sql = self.get_query(src, fix=True, **kwargs)
        DEFAULT_LOGGER.debug("exec query", extra={"label": self, "sql": sql})
        return self.spark.sql(sql)

    def get_query_context(self, src: AllowedSources, **kwargs: Any) -> dict:  # noqa: ANN401 - options bag forwarded through the cdc query pipeline
        DEFAULT_LOGGER.debug("deduce query context", extra={"label": self})

        format = _get_format(src)

        inputs = self.get_columns(src, backtick=False, sort=False)
        fields = [c for c in inputs if not c.startswith("__")]
        keys = kwargs.get("keys")

        mode = kwargs.get("mode", "complete")
        tgt = str(self.table) if mode == "update" or (mode == "append" and "__timestamp" in inputs) else None

        exclude = kwargs.get("exclude", [])  # used by silver to exclude __operation from output if not update
        cast = kwargs.get("cast", {})  # used by silver to cast columns to target types

        order_duplicate_by = kwargs.get("order_duplicate_by")
        if order_duplicate_by:
            order_duplicate_by = [f"{key} {value}" for key, value in order_duplicate_by.items()]

        add_source = kwargs.get("add_source")
        add_calculated_columns = kwargs.get("add_calculated_columns", [])
        if add_calculated_columns:
            raise ValueError("add_calculated_columns is not yet supported")
        add_operation = kwargs.get("add_operation")
        add_key = kwargs.get("add_key")
        add_hash = kwargs.get("add_hash")
        add_timestamp = kwargs.get("add_timestamp")
        add_last_updated = kwargs.get("add_last_updated")
        add_metadata = kwargs.get("add_metadata")

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

        soft_delete = kwargs.get("soft_delete")
        delete_missing = kwargs.get("delete_missing")
        slice = kwargs.get("slice")
        rectify = kwargs.get("rectify")
        deduplicate = kwargs.get("deduplicate")
        deduplicate_key = kwargs.get("deduplicate_key")
        deduplicate_hash = kwargs.get("deduplicate_hash")
        correct_valid_from = kwargs.get("correct_valid_from")

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

        deduplicate, deduplicate_key, deduplicate_hash = _resolve_deduplication(
            deduplicate,
            deduplicate_key,
            deduplicate_hash,
            default=bool(self.slowly_changing_dimension),
            ordered=bool(order_duplicate_by),
        )

        # always rectify if not set
        if self.slowly_changing_dimension and rectify is None:
            rectify = True

        # only correct valid_from on first load
        if self.slowly_changing_dimension and mode == "update":
            correct_valid_from = correct_valid_from and not has_rows

        # override slice for incremental load if timestamp and rows are present
        if slice is None and mode == "update" and has_timestamp and has_rows:
            slice = "update"

        # override slice for full load if update and table is empty
        if slice == "update" and not has_rows:
            slice = None

        # A "latest" slice over a source with no rows generates invalid SQL (issue #182): there is nothing to take
        # the latest of, and an aggregate MAX() over zero rows still yields one NULL row.
        if slice == "latest" and not self.has_data(src):
            slice = None

        truncate_as_reload = (
            "__operation" in inputs and not add_operation and self.change_data_capture in ["scd1", "scd2"]
        )
        # A 'truncate' row (issue #66) is rewritten to 'reload' so it reuses rectify's per-key "not found in next
        # reload" reconciliation instead of a new code path. Moot when add_operation forces the column to a constant,
        # and meaningless outside scd1/scd2 (nocdc/scd0 have no rectify pipeline and don't always carry a __key).
        overwrite = _get_overwrite(
            inputs,
            truncate_as_reload=truncate_as_reload,
            add_operation=add_operation,
            add_timestamp=add_timestamp,
            add_key=add_key,
            add_hash=add_hash,
            add_last_updated=add_last_updated,
            add_metadata=add_metadata,
        )
        if "__timestamp" in inputs and not add_timestamp:
            cast["__timestamp"] = "timestamp"

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

        intermediates, outputs = self._get_layout(
            inputs,
            fields,
            exclude,
            has_operation=has_operation,
            has_timestamp=has_timestamp,
            has_key=has_key,
            has_hash=has_hash,
            has_metadata=has_metadata,
            has_last_updated=has_last_updated,
            has_source=has_source,
            has_identity=has_identity,
            has_rescued_data=has_rescued_data,
            soft_delete=soft_delete,
            advanced_ctes=advanced_ctes,
        )
        parents = _get_parents(slice, rectify, deduplicate_key, deduplicate_hash)

        return {
            "debugmode": IS_DEBUGMODE,
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
            "advanced_deduplication": advanced_deduplication,
            # cte's
            "slice": slice,
            "rectify": rectify,
            "deduplicate": deduplicate,
            "deduplicate_key": deduplicate_key,
            "deduplicate_hash": deduplicate_hash,
            # has
            "has_no_data": has_no_data,
            "has_rows": has_rows,
            "has_source": has_source,
            "has_metadata": has_metadata,
            "has_last_updated": has_last_updated,
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
            "add_last_updated": add_last_updated,
            "add_key": add_key,
            "add_hash": add_hash,
            # value add
            "add_operation": add_operation,
            "truncate_as_reload": truncate_as_reload,
            "add_source": add_source,
            "add_calculated_columns": add_calculated_columns,
            # extra
            "order_duplicate_by": order_duplicate_by,
            "soft_delete": soft_delete,
            "correct_valid_from": correct_valid_from,
            # overwrite
            "overwrite": overwrite,
            # cast
            "cast": cast,
            # filter
            "slices": None,
            "sources": None,
            "filter_where": kwargs.get("filter_where"),
            "update_where": kwargs.get("update_where"),
            # parents
            **parents,
        }

    def _get_layout(  # noqa: PLR0913
        self,
        inputs: list[str],
        fields: list[str],
        exclude: list[str],
        *,
        has_operation: Any,  # noqa: ANN401 - flag or add_* value
        has_timestamp: Any,  # noqa: ANN401
        has_key: Any,  # noqa: ANN401
        has_hash: Any,  # noqa: ANN401
        has_metadata: Any,  # noqa: ANN401
        has_last_updated: Any,  # noqa: ANN401
        has_source: Any,  # noqa: ANN401
        has_identity: bool,
        has_rescued_data: bool,
        soft_delete: Any,  # noqa: ANN401
        advanced_ctes: Any,  # noqa: ANN401
    ) -> tuple[list[str], list[str]]:
        if self.change_data_capture == "nocdc":
            intermediates = list(inputs)
            outputs = list(inputs)
        else:
            intermediates = list(fields)
            outputs = list(fields)

        for has, name in [
            (has_operation, "__operation"),
            (has_timestamp, "__timestamp"),
            (has_key, "__key"),
            (has_hash, "__hash"),
        ]:
            if has and name not in outputs:
                outputs.append(name)

        for has, name in [
            (has_metadata, "__metadata"),
            (has_last_updated, "__last_updated"),
            (has_source, "__source"),
            (has_identity, "__identity"),
            (has_rescued_data, "__rescued_data"),
        ]:
            if has:
                if name not in outputs:
                    outputs.append(name)
                if name not in intermediates:
                    intermediates.append(name)

        extra = ["__is_deleted", "__is_current"] if soft_delete else []
        if self.change_data_capture == "scd2":
            extra += ["__valid_from", "__valid_to", "__is_current"]
        outputs += [n for n in dict.fromkeys(extra) if n not in outputs]

        if advanced_ctes:
            intermediates += [n for n in ["__operation", "__timestamp"] if n not in intermediates]

        # needed for deduplication and/or rectification
        # might need __operation or __source
        intermediates += [n for n in ["__key", "__hash"] if n not in intermediates]

        outputs = [o for o in outputs if o not in exclude]
        return intermediates, self.sort_columns(outputs)

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

    def _probe_source_ref(self, context: dict) -> str:
        # Mirrors ctes/base.sql.jinja's FROM-clause branches. Real callers only pass format "table" or "query"; the
        # "dataframe" branch is kept for parity with the template and with tests that call get_query() with a mock
        # DataFrame.
        format = context["format"]
        if format == "query":
            src_ref = f"({context['src']})"
        elif format == "dataframe":
            src_ref = "{src}"
        else:
            src_ref = str(context["src"])

        filter_where = context["filter_where"]
        if filter_where:
            src_ref = f"(select * from {src_ref} where {filter_where})"

        return src_ref

    def _probe_sql(self, context: dict) -> str:
        # __timestamp must be a real timestamp for slice="latest" (the only cast get_query_context sets, as in
        # ctes/base.sql.jinja). add_timestamp with slice="latest" has no per-row timestamp to take the latest of and
        # no real caller does that, so it is not handled here.
        environment = Environment(loader=PackageLoader("fabricks.cdc", "templates"))
        template = environment.get_template("probe.sql.jinja")
        return template.render(
            slice=context["slice"],
            has_source=context["has_source"],
            src_ref=self._probe_source_ref(context),
            tgt=context["tgt"],
            timestamp_col="__valid_from" if context["cdc"] == "scd2" else "__timestamp",
        )

    def fix_context(self, context: dict, fix: bool | None = True, **_kwargs: Any) -> dict:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        try:
            sql = self._probe_sql(context)
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

    def _materialize_current_view(self, environment: Environment, context: dict) -> str:
        # current_view feeds several consumers (rectify, the scd1/scd2 merge-key anti-join), each pruning different
        # columns, which defeats Spark's CTE reuse and re-reads the target once per consumer (issue #202). Cache the
        # projected rows once under a stable global temp view; lazy, so the scan runs inside the merge query's own job.
        short_name = f"{self.qualified_name}__current"
        sql = fix_sql(environment.get_template("ctes/current.sql.jinja").render(**context))
        self.spark.sql(f"uncache table if exists global_temp.{short_name}")
        view = create_or_replace_global_temp_view(short_name, self.spark.sql(sql), job=self)
        self.spark.sql(f"cache lazy table {view}")
        return view

    def get_query(self, src: AllowedSources, fix: bool | None = True, **kwargs: Any) -> str:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        context = self.get_query_context(src=src, **kwargs)
        environment = Environment(loader=PackageLoader("fabricks.cdc", "templates"))

        try:
            if context.get("slice"):
                context = self.fix_context(context, fix=fix, **kwargs)

            # the queries read `current_view` exactly when `mode == "update"` and
            # `has_rows` -- keep these conditions in sync.
            if context.get("mode") == "update" and context.get("has_rows"):
                context["current_view"] = self._materialize_current_view(environment, context)

            template = environment.get_template("query.sql.jinja")

            sql = template.render(**context)
            if fix:
                sql = self.fix_sql(sql)
            else:
                DEFAULT_LOGGER.debug("print query", extra={"label": self, "sql": sql})

        except (Exception, TypeError) as e:
            DEFAULT_LOGGER.debug("context", extra={"label": self, "context": context})
            DEFAULT_LOGGER.exception("fail to render sql query", extra={"label": self, "context": context})
            raise e

        return sql

    def append(self, src: AllowedSources, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        if not self.table.registered:
            self.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        df = self.reorder_dataframe(df)

        name = f"{self.qualified_name}__append"
        create_or_replace_global_temp_view(name, df, uuid=kwargs.get("uuid", False), job=self)
        append = f"insert into table {self.table} by name select * from global_temp.{name}"

        DEFAULT_LOGGER.debug("exec append", extra={"label": self, "sql": append})
        self.spark.sql(append)

    def overwrite(self, src: AllowedSources, dynamic: bool | None = False, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        if not self.table.registered:
            self.create_table(src, **kwargs)

        df = self.get_data(src, **kwargs)
        df = self.reorder_dataframe(df)

        if not dynamic and kwargs.get("update_where"):
            dynamic = True

        if dynamic:
            self.spark.sql("set spark.sql.sources.partitionOverwriteMode = dynamic")

        name = f"{self.qualified_name}__overwrite"
        create_or_replace_global_temp_view(name, df, uuid=kwargs.get("uuid", False), job=self)
        overwrite = f"insert overwrite table {self.table} by name select * from global_temp.{name}"

        DEFAULT_LOGGER.debug("excec overwrite", extra={"label": self, "sql": overwrite})
        self.spark.sql(overwrite)
