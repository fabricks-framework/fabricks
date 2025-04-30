from typing import List, Literal, Type, TypeVar, Union, get_args, get_origin

import yaml
from pydantic import BaseModel as PydanticBaseModel
from pydantic import parse_obj_as
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    Row,
    StringType,
    StructField,
    StructType,
)

from fabricks.context import SPARK

types_ = {
    str: StringType(),
    bool: BooleanType(),
    float: DoubleType(),
    int: LongType(),
    dict: MapType(StringType(), StringType()),
}
T = TypeVar("T")


def _to_spark_type(type_):
    if type_ in types_:
        return types_[type_]

    origin = get_origin(type_)
    args = get_args(type_)
    if origin is Literal:
        return StringType()
    if origin is list:
        return ArrayType(_to_spark_type(args[0]))
    if origin is dict:
        return MapType(
            _to_spark_type(args[0]),
            _to_spark_type(args[1]),
        )

    if issubclass(type_, PydanticBaseModel):
        return _schema_pyspark(type_)

    raise ValueError(type_)


def _schema_pyspark(model):
    fields = []
    for field in model.__fields__.values():
        type_ = field.outer_type_
        spark_type_ = _to_spark_type(type_)
        f = StructField(
            name=field.name,
            dataType=spark_type_,  # type: ignore
            nullable=not field.required,
        )
        fields.append(f)
    return StructType(fields)


class FBaseModel(PydanticBaseModel):
    @classmethod
    def from_yaml(cls: Type[T], path: str) -> Union[T, List[T]]:
        with open(path, encoding="utf-8") as f:
            y = yaml.safe_load(f)
            if isinstance(y, List):
                return parse_obj_as(List[cls], y)
            else:
                return parse_obj_as(cls, y)

    @classmethod
    def from_row(cls: Type[T], row: Row) -> T:
        return parse_obj_as(cls, row.asDict(True))

    @classmethod
    def from_dataframe(cls: Type[T], df: DataFrame) -> List[T]:
        return [parse_obj_as(cls, row.asDict(True)) for row in df.collect()]

    def schema_pyspark(self):
        return _schema_pyspark(self)

    @staticmethod
    def get_dataframe(data: Union[T, List[T]]) -> DataFrame:
        if isinstance(data, List):
            df = SPARK.createDataFrame([d.dict() for d in data], data[0].schema_pyspark())  # type: ignore
        else:
            df = SPARK.createDataFrame([data.dict()], data.schema_pyspark())  # type: ignore
        return df
