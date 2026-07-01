from typing import Union

from pyspark.sql import DataFrame

from fabricks.cdc.base import BaseCDC
from fabricks.metastore.table import Table
from fabricks.models.cdc import CdcContext


class SCD(BaseCDC):
    def delete_missing(self, src: Union[DataFrame, Table, str], context: CdcContext):
        self.merge(
            src,
            context.model_copy(
                update={"add_operation": "reload", "delete_missing": True, "mode": "update"},
            ),
        )

    def complete(self, src: Union[DataFrame, Table, str], context: CdcContext):
        self.overwrite(src, context=context.model_copy(update={"mode": "complete"}))

    def update(self, src: Union[DataFrame, Table, str], context: CdcContext):
        self.merge(src, context.model_copy(update={"mode": "update"}))
