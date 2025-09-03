from typing import Sequence

from pyspark.sql import DataFrame

from fabricks.metastore.table import SchemaDiff


class CustomException(Exception):
    pass


class CheckException(Exception):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(self.message)


class CheckWarning(CheckException):
    pass


class PreRunCheckException(CheckException):
    pass


class PostRunCheckException(CheckException):
    pass


class PreRunCheckWarning(CheckWarning):
    pass


class PostRunCheckWarning(CheckWarning):
    pass


class PreRunInvokeException(CustomException):
    pass


class PostRunInvokeException(CustomException):
    pass


class SkipRunCheckWarning(CheckWarning):
    pass


class SchemaDriftException(Exception):
    @staticmethod
    def from_diffs(table: str, diffs: Sequence[SchemaDiff]):
        diff_strs = []

        added = [d.new_column or d.column for d in diffs if d.status == "added"]
        if added:
            diff_strs.append(f"Added columns: {', '.join(added)}")

        removed = [d.column for d in diffs if d.status == "dropped"]
        if removed:
            diff_strs.append(f"Removed columns: {', '.join(removed)}")

        changed = [f"{d.column} ({d.data_type} -> {d.new_data_type})" for d in diffs if d.status == "changed"]
        if changed:
            diff_strs.append(f"Changed columns: {', '.join(changed)}")

        diff_str = "\r\n ".join(diff_strs)

        return SchemaDriftException(f"Schema drift detected in table {table}:\n {diff_str}", diffs)

    def __init__(self, message: str, diffs: Sequence[SchemaDiff]):
        super().__init__(message)
        self.diffs = diffs
