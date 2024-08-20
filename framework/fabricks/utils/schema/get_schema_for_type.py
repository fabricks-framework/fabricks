import dataclasses
from typing import List, Literal, Type, Union, cast, get_type_hints, overload

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
)


@overload
def get_schema_for_type(proptype: Union[int, str, float, bool]) -> DataType: ...


@overload
def get_schema_for_type(proptype: Type) -> StructType: ...


def _merge_struct_types(types: List[DataType]):
    not_none_types = [t for t in types if type(t) != type(NullType())]  # noqa: E721

    assert len([f for f in not_none_types if not isinstance(f, StructType)]) == 0
    all_fields: List[StructField] = []

    for subtype in not_none_types:
        fields = cast(StructType, subtype).fields
        for field in fields:
            existing_field = next((f for f in all_fields if f.name == field.name), None)
            if existing_field is not None and (
                type(existing_field.dataType) != type(field.dataType)  # noqa: E721
                or isinstance(existing_field.dataType, StructType)
            ):
                new_type = _merge_struct_types([existing_field.dataType, field.dataType])
                all_fields.append(StructField(name=field.name, dataType=new_type))
                all_fields.remove(existing_field)
            else:
                assert existing_field is None or type(existing_field.dataType) == type(field.dataType)  # noqa: E721
                if existing_field is None:
                    all_fields.append(field)

    return StructType(fields=all_fields)


def get_schema_for_type(proptype: Type) -> DataType:  # type: ignore
    if hasattr(proptype, "__origin__") and proptype.__origin__ == Literal:
        return get_schema_for_type(type(proptype.__args__[0]))  # For literal types we assume first type is correct

    if hasattr(proptype, "__origin__") and proptype.__origin__ == Union:
        if len(proptype.__args__) == 2 and proptype.__args__[0] == type(None):  # noqa E721
            return get_schema_for_type(proptype.__args__[1])
        if len(proptype.__args__) == 2 and proptype.__args__[1] == type(None):  # noqa E721
            return get_schema_for_type(proptype.__args__[0])

        return _merge_struct_types([get_schema_for_type(f) for f in proptype.__args__])

    if proptype == type(None):  # noqa E721
        return NullType()

    if proptype == str:  # noqa E721
        return StringType()

    if proptype == int:  # noqa E721
        return LongType()

    if proptype == float:  # noqa E721
        return DoubleType()

    if proptype == bool:  # noqa E721
        return BooleanType()

    if hasattr(proptype, "__origin__") and proptype.__origin__ == list:  # noqa E721
        return ArrayType(get_schema_for_type(proptype.__args__[0]))

    if proptype == dict[str, str]:
        return MapType(StringType(), StringType())

    if hasattr(proptype, "__bases__") and len(proptype.__bases__) == 1 and proptype.__bases__[0] == dict:  # noqa E721
        types = get_type_hints(proptype)
        fields = [StructField(k, get_schema_for_type(v)) for k, v in types.items()]
        return StructType(fields=fields)

    if dataclasses.is_dataclass(proptype):
        fields = [StructField(f.name, get_schema_for_type(f.type)) for f in dataclasses.fields(proptype)]
        return StructType(fields=fields)

    raise NotImplementedError()
