from typing import Any

from pyspark.sql import DataFrame

from fabricks.cdc.base import BaseCDC
from fabricks.metastore.table import Table


class SCD(BaseCDC):
    def delete_missing(self, src: DataFrame | Table | str, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        kwargs["add_operation"] = "reload"
        kwargs["delete_missing"] = True
        kwargs["mode"] = "update"
        self.merge(src, **kwargs)

    def complete(self, src: DataFrame | Table | str, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        kwargs["mode"] = "complete"
        self.overwrite(src, **kwargs)

    def update(self, src: DataFrame | Table | str, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        kwargs["mode"] = "update"
        self.merge(src, **kwargs)
