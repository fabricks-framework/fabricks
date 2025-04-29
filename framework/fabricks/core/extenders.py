from importlib.util import module_from_spec, spec_from_file_location
from typing import Callable

from fabricks.context import IS_UNITY_CATALOG, PATH_EXTENDERS
from fabricks.context.log import DEFAULT_LOGGER

EXTENDERS: dict[str, Callable] = {}


def get_extender(name: str) -> Callable:
    path = PATH_EXTENDERS.join(f"{name}.py")
    if not IS_UNITY_CATALOG:
        assert path.exists(), "no valid extender found in {path.string}"
    else:
        DEFAULT_LOGGER.debug(f"could not check if extender exists ({path.string})")

    spec = spec_from_file_location(name, path.string)
    assert spec, "no valid extender found in {path.string}"
    assert spec.loader is not None

    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    e = EXTENDERS[name]

    return e


def extender(name: str):
    def decorator(fn: Callable):
        EXTENDERS[name] = fn
        return fn

    return decorator
