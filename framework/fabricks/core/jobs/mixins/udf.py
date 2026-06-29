import re
from typing import Optional

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.mixins._protocol import JobProtocol
from fabricks.core.udfs import UDF_PREFIX, is_registered, register_udf

_UDF_PATTERN = re.compile(rf"(?<={UDF_PREFIX})\w*(?=\()")


class UDFMixin(JobProtocol):
    def get_udfs(self) -> Optional[list[str]]:
        updated_columns = self.updater_options.columns if self.updater_options else {}
        if updated_columns:
            udfs = []

            for value in updated_columns.values():
                matches = self._match_udfs(value)
                if matches:
                    udfs += matches

            return list(set(udfs))

    def register_udfs(self, force: bool | None = False):
        if not self._udf_registered or force:
            udfs = self.get_udfs()
            if udfs:
                for u in udfs:
                    if not is_registered(u, self.spark):
                        DEFAULT_LOGGER.debug(f"register udf {u}", extra={"label": self})
                        register_udf(u, spark=self.spark)
            self._udf_registered = True

    def _match_udfs(self, string: str) -> Optional[list[str]]:
        if UDF_PREFIX in string:
            matches = _UDF_PATTERN.findall(string)
            return list(set(matches)) if matches else None
