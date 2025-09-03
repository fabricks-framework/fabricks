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
        out = []

        added = [d.new_column or d.column for d in diffs if d.status == "added"]
        if added:
            out.append("Added columns:\n" + "\n".join(f"\t{col}" for col in added))

        removed = [d.column for d in diffs if d.status == "dropped"]
        if removed:
            out.append("Removed columns:\n" + "\n".join(f"\t{col}" for col in removed))

        changed = [f"{d.column} ({d.data_type} -> {d.new_data_type})" for d in diffs if d.status == "changed"]
        if changed:
            out.append("Changed columns:\n" + "\n".join(f"\t{col}" for col in changed))

        out = "\n".join(out)

        return SchemaDriftException(f"Schema drift detected in table {table}:\n {out}", diffs)

    def __init__(self, message: str, diffs: Sequence[SchemaDiff]):
        super().__init__(message)
        self.diffs = diffs
