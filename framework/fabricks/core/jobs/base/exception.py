from collections.abc import Sequence
from typing import ClassVar

from pyspark.sql import DataFrame

from fabricks.metastore.table import SchemaDiff


class CustomError(Exception):
    pass


class CheckError(Exception):
    is_stale: ClassVar[bool] = False
    should_restore: ClassVar[bool] = False

    def __init__(self, message: str, dataframe: DataFrame | None = None) -> None:
        self.message = message
        self.dataframe = dataframe

        super().__init__(self.message)


class CheckWarning(CheckError):  # noqa: N818 - warning, not an error; kept as-is (widely used name, not part of this cleanup)
    is_stale: ClassVar[bool] = True


class PreRunCheckException(CheckError):  # noqa: N818 - widely used name across the codebase, not part of this cleanup
    should_restore: ClassVar[bool] = True


class PostRunCheckException(CheckError):  # noqa: N818 - widely used name across the codebase, not part of this cleanup
    should_restore: ClassVar[bool] = True


class PreRunCheckWarning(CheckWarning):
    pass


class PostRunCheckWarning(CheckWarning):
    pass


class InvokeException(CustomError):  # noqa: N818 - matches CustomError/CheckError's naming, not part of this cleanup
    pass


class PreRunInvokeException(InvokeException):
    pass


class PostRunInvokeException(InvokeException):
    pass


class SkipWarning(CheckError):  # noqa: N818 - warning, not an error; kept as-is (widely used name, not part of this cleanup)
    is_stale: ClassVar[bool] = True


class SkipRunCheckWarning(SkipWarning):
    pass


class SkipRunTimeWarning(SkipWarning):
    pass


class UnchangedWarning(CheckWarning):
    """Raised when a job ran (or would have run) but found no new upstream
    data -- distinct from SkipWarning, which means the job was explicitly
    configured not to run at all (check_options.skip / time-window)."""


class SchemaDriftError(Exception):
    @staticmethod
    def from_diffs(_table: str, diffs: Sequence[SchemaDiff]) -> "SchemaDriftError":
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
            return SchemaDriftError(f"type widening detected:\n {out}", diffs, type_widening_compatible)
        return SchemaDriftError(f"schema drift detected:\n {out}", diffs, type_widening_compatible)

    def __init__(self, message: str, diffs: Sequence[SchemaDiff], type_widening_compatible: bool = False) -> None:
        super().__init__(message)
        self.diffs = diffs
        self.type_widening_compatible = type_widening_compatible
