import dataclasses
import datetime
import logging
import sys
import types
from typing import Any, ForwardRef, Literal, Type, Union, get_type_hints
from uuid import UUID

LOGGER = logging.getLogger(__name__)


def get_json_schema_for_type(proptype: Type):
    def_list: dict[str, dict] = {}
    schema = _get_json_schema_for_type(proptype, def_list, is_root=True)
    schema["$defs"] = def_list
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return schema


def _get_json_schema_for_type(proptype: Type, def_list: dict[str, dict], is_root: bool, is_nullable=False) -> dict:
    def _fixref(input: dict) -> dict:
        if "type" in input:
            if "$ref" in input["type"]:
                return input["type"]
        return input

    def _may_null(input: dict, is_nullable: bool) -> dict:
        if is_nullable:
            return {"oneOf": [{"type": "null"}, input]}
        return input

    if hasattr(proptype, "__origin__") and proptype.__origin__ == Literal:
        return {"enum": proptype.__args__}

    if hasattr(proptype, "__origin__") and proptype.__origin__ == tuple:  # noqa E721
        return {
            "type": "array",
            "minItems": len(proptype.__args__),
            "maxItems": len(proptype.__args__),
            "additionalItems": False,
            "prefixItems": [_get_json_schema_for_type(t, def_list, is_root=False) for t in proptype.__args__],
        }

    if (sys.version_info >= (3, 10) and isinstance(proptype, types.UnionType)) or (
        hasattr(proptype, "__origin__") and proptype.__origin__ == Union
    ):
        if len(proptype.__args__) == 2 and proptype.__args__[0] == type(None):  # noqa E721
            t = _get_json_schema_for_type(proptype.__args__[1], def_list, is_root=False, is_nullable=True)
            return t

        if len(proptype.__args__) == 2 and proptype.__args__[1] == type(None):  # noqa E721
            t = _get_json_schema_for_type(proptype.__args__[0], def_list, is_root=False, is_nullable=True)
            return t

        one_of_types = [
            _get_json_schema_for_type(f, def_list, is_root=False, is_nullable=False) for f in proptype.__args__
        ]

        return {"oneOf": one_of_types}

    if proptype == type(None):  # noqa E721
        return {"type": "null"}

    if proptype == str:  # noqa E721
        return {"type": "string"} if not is_nullable else {"type": ["string", "null"]}

    if proptype == Any:
        return {}

    if proptype == UUID:
        return {
            "type": "string" if not is_nullable else ["string", "null"],
            "format": "uuid",
        }

    if proptype == int:  # noqa E721
        return {"type": "integer" if not is_nullable else ["integer", "null"]}

    if proptype == float:  # noqa E721
        return {"type": "number" if not is_nullable else ["number", "null"]}

    if proptype == bool:  # noqa E721
        return {"type": "boolean" if not is_nullable else ["boolean", "null"]}

    if hasattr(proptype, "__origin__") and proptype.__origin__ == list:  # noqa E721
        return {
            "type": "array",
            "items": _get_json_schema_for_type(proptype.__args__[0], def_list, is_root=False),
        }

    if hasattr(proptype, "__bases__") and len(proptype.__bases__) == 1 and proptype.__bases__[0] == dict:  # noqa E721
        typehints = get_type_hints(proptype)
        props = {k: _get_json_schema_for_type(v, def_list, is_root=False) for (k, v) in typehints.items()}

        if hasattr(proptype, "__name__") and not is_root:
            def_list[proptype.__name__] = {"type": "object", "properties": props}
            return _may_null({"$ref": "#/$defs/" + proptype.__name__}, is_nullable)
        else:
            return _may_null({"type": "object", "properties": props}, is_nullable)

    if dataclasses.is_dataclass(proptype):
        required = [
            f.name
            for f in dataclasses.fields(proptype)
            if f.default == dataclasses.MISSING and f.default_factory == dataclasses.MISSING and f.init
        ]
        definition = {
            "type": "object",
            "required": required,
            "additionalProperties": False,
            "properties": {
                f.name: _get_json_schema_for_type(f.type, def_list, is_root=False)  # type: ignore
                for f in dataclasses.fields(proptype)
            },
        }

        if is_root:
            return definition
        else:
            def_list[proptype.__name__] = definition

            return _may_null({"$ref": "#/$defs/" + proptype.__name__}, is_nullable)

    if hasattr(proptype, "__origin__") and proptype.__origin__ == dict and len(proptype.__args__) == 2:  # noqa E721
        keytype = proptype.__args__[0]
        if keytype != str and keytype != UUID:  # noqa E721
            raise NotImplementedError()
        valuetype = proptype.__args__[1]
        return _may_null(
            {
                "type": "object",
                "additionalProperties": _fixref(
                    {"type": _get_json_schema_for_type(valuetype, def_list, is_root=False)}
                ),
            },
            is_nullable,
        )

    if isinstance(proptype, ForwardRef):
        arg = proptype.__forward_arg__
        return _may_null({"$ref": "#/$defs/" + arg}, is_nullable)

    if proptype == datetime.datetime:
        return {
            "type": "string" if not is_nullable else ["string", "null"],
            "format": "date-time",
        }

    if proptype == datetime.time:
        return {
            "type": "string" if not is_nullable else ["string", "null"],
            "format": "time",
        }

    if proptype == datetime.date:
        return {
            "type": "string" if not is_nullable else ["string", "null"],
            "format": "date",
        }

    return {}
