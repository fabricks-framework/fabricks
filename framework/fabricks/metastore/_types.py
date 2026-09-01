from typing import Literal

from pydantic import BaseModel


class SchemaDiff(BaseModel):
    column: str
    data_type: str | None = None
    new_column: str | None = None
    new_data_type: str | None = None
    status: Literal["added", "changed", "dropped"]

    @property
    def type_widening_compatible(self) -> bool:
        if self.status != "changed":
            return False

        assert self.new_data_type
        assert self.data_type
        map = {
            "byte": {"short", "int", "long", "decimal", "double"},
            "short": {"int", "long", "decimal", "double"},
            "int": {"long", "decimal", "double"},
            "long": {"decimal"},
            "float": {"double"},
        }
        return self.new_data_type.lower() in map.get(self.data_type.lower(), set())


class DroppedColumn(SchemaDiff):
    def __init__(self, column: str, data_type: str | None = None) -> None:
        super().__init__(column=column, data_type=data_type, status="dropped")

    def __str__(self) -> str:
        return f"dropped {self.column}"


class AddedColumn(SchemaDiff):
    def __init__(self, new_column: str, new_data_type: str) -> None:
        super().__init__(column=new_column, new_column=new_column, new_data_type=new_data_type, status="added")

    def __str__(self) -> str:
        return f"added {self.new_column} with type {self.new_data_type}"


class ChangedColumn(SchemaDiff):
    def __init__(self, column: str, data_type: str, new_data_type: str) -> None:
        super().__init__(column=column, data_type=data_type, new_data_type=new_data_type, status="changed")

    def __str__(self) -> str:
        return (
            f"changed {self.column} from {self.data_type} to {self.new_data_type} "
            f"(widening compatible: {self.type_widening_compatible})"
        )
