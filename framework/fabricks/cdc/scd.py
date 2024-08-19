from typing import Union

from pyspark.sql import DataFrame

from fabricks.cdc.base import BaseCDC
from fabricks.metastore.table import Table


class SCD(BaseCDC):
    def delete_missing(self, src: Union[DataFrame, Table, str], **kwargs):
        kwargs["add_operation"] = "reload"
        kwargs["mode"] = "update"
        self.merge(src, **kwargs)

    def complete(self, src: Union[DataFrame, Table, str], **kwargs):
        kwargs["mode"] = "complete"
        self.overwrite(src, **kwargs)

    def update(self, src: Union[DataFrame, Table, str], **kwargs):
        kwargs["mode"] = "update"
        self.merge(src, **kwargs)
