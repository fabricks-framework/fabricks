from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

from jinja2 import Environment, PackageLoader
from pyspark.sql import DataFrame

from fabricks.cdc.config import AllowedSources
from fabricks.context.config import IS_DEBUGMODE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.view import create_or_replace_global_temp_view
from fabricks.models.cdc import CDCMergeContext
from fabricks.utils._types import DataFrameLike
from fabricks.utils.helpers import backticks

if TYPE_CHECKING:
    from fabricks.cdc.base import BaseCDC

_ENV = Environment(loader=PackageLoader("fabricks.cdc", "templates"))


class CDCMerger:
    """Owns merge context building, merge SQL rendering, and the merge write path."""

    def __init__(self, cdc: BaseCDC):
        self._cdc = cdc

    def get_merge_context(self, src: Union[DataFrame, str], **kwargs) -> CDCMergeContext:
        cdc = self._cdc

        if isinstance(src, DataFrameLike):
            format = "dataframe"
            columns = cdc.get_columns(src, backtick=False, sort=False, check=False)

        elif isinstance(src, str):
            format = "view"
            columns = cdc.get_columns(f"select * from {src}", backtick=False, sort=False, check=False)

        else:
            raise ValueError(f"{src} not allowed")

        assert "__merge_key" in columns, "__merge_key not found"
        assert "__merge_condition" in columns, "__merge_condition not found"

        keys = kwargs.get("keys")
        if isinstance(keys, str):
            keys = [keys]

        columns = [c for c in columns if c not in ["__merge_condition", "__merge_key"]]
        fields = [c for c in columns if not c.startswith("__")]
        where = kwargs.get("update_where") if cdc.table.rows > 0 else None
        soft_delete = "__is_deleted" in columns

        has_source = "__source" in columns
        has_key = "__key" in columns
        has_metadata = "__metadata" in columns
        has_hash = "__hash" in columns
        has_timestamp = "__timestamp" in columns
        has_identity = "__identity" in columns

        if keys:
            keys = backticks(keys)
        if columns:
            columns = backticks(columns)
        if fields:
            fields = backticks(fields)

        assert has_key or keys, f"{cdc} - __key or keys not found"

        return CDCMergeContext(
            template="merge",
            debugmode=IS_DEBUGMODE,
            src=src,
            format=format,
            tgt=cdc.table,
            cdc=cdc.change_data_capture,
            columns=columns,
            fields=fields,
            soft_delete=soft_delete,
            has_source=has_source,
            has_identity=has_identity,
            has_key=has_key,
            has_hash=has_hash,
            keys=keys,
            has_metadata=has_metadata,
            has_timestamp=has_timestamp,
            where=where,
        )

    def get_merge_query(self, src: Union[DataFrame, str], fix: Optional[bool] = True, **kwargs) -> str:
        cdc = self._cdc
        context = self.get_merge_context(src=src, **kwargs)

        try:
            sql = _ENV.get_template("merge.sql.jinja").render(**vars(context))
        except Exception as e:
            DEFAULT_LOGGER.debug("context", extra={"label": cdc, "content": context})
            raise e

        if fix:
            sql = cdc.fix_sql(sql)

        return sql

    def merge(self, src: AllowedSources, **kwargs):
        cdc = self._cdc
        if not cdc.table.exists():
            cdc.create_table(src, **kwargs)

        df = cdc.get_data(src, **kwargs)
        global_temp_view = f"{cdc.qualified_name}__merge"
        view = create_or_replace_global_temp_view(global_temp_view, df, uuid=kwargs.get("uuid", False), job=cdc)

        merge = self.get_merge_query(view, **kwargs)
        DEFAULT_LOGGER.debug("exec merge", extra={"label": cdc, "sql": merge})
        cdc.spark.sql(merge, src=view)
