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
        type_widening_compatible = True

        added = [d.new_column or d.column for d in diffs if d.status == "added"]
        if added:
            type_widening_compatible = False
            out.append("added columns:\n" + "\n".join(f"\t- {col}" for col in added))

        removed = [d.column for d in diffs if d.status == "dropped"]
        if removed:
            type_widening_compatible = False
            out.append("removed columns:\n" + "\n".join(f"\t- {col}" for col in removed))

        changed = [f"{d.column} ({d.data_type} -> {d.new_data_type})" for d in diffs if d.status == "changed"]
        if changed:
            if False in [d.type_widening_compatible for d in diffs if d.status == "changed"]:
                type_widening_compatible = False

            out.append("changed columns:\n" + "\n".join(f"\t- {col}" for col in changed))

        out = "\n".join(out)

        if type_widening_compatible:
            return SchemaDriftException(f"type widening detected:\n {out}", diffs, type_widening_compatible)
        else:
            return SchemaDriftException(f"schema drift detected:\n {out}", diffs, type_widening_compatible)

    def __init__(self, message: str, diffs: Sequence[SchemaDiff], type_widening_compatible: bool = False):
        super().__init__(message)
        self.diffs = diffs
        self.type_widening_compatible = type_widening_compatible
