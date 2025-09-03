from typing import Literal, Optional

from pydantic import BaseModel


class SchemaDiff(BaseModel):
    column: str
    data_type: Optional[str] = None
    new_column: Optional[str] = None
    new_data_type: Optional[str] = None
    status: Literal["added", "changed", "dropped"]


class DroppedColumn(SchemaDiff):
    def __init__(self, column: str, data_type: Optional[str] = None):
        super().__init__(
            column=column,
            data_type=data_type,
            status="dropped",
        )

    def __str__(self):
        return f"Dropped {self.column}"


class AddedColumn(SchemaDiff):
    def __init__(self, new_column: str, new_data_type: str):
        super().__init__(
            column=new_column,
            new_column=new_column,
            new_data_type=new_data_type,
            status="added",
        )

    def __str__(self):
        return f"Added {self.new_column} with type {self.new_data_type}"


class ChangedColumn(SchemaDiff):
    def __init__(self, column: str, data_type: str, new_data_type: str):
        super().__init__(
            column=column,
            data_type=data_type,
            new_data_type=new_data_type,
            status="changed",
        )

        return f"Changed {self.column} from {self.data_type} to {self.new_data_type}"
