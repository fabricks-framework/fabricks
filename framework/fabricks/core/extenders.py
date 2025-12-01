from typing import Callable

from fabricks.context import IS_UNITY_CATALOG, PATH_EXTENDERS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import load_module_from_path

EXTENDERS: dict[str, Callable] = {}


def get_extender(name: str) -> Callable:
    path = PATH_EXTENDERS.joinpath(f"{name}.py")
    if not IS_UNITY_CATALOG:
        assert path.exists(), "no valid extender found in {path.string}"
    else:
        DEFAULT_LOGGER.debug(f"could not check if extender exists ({path.string})")

    load_module_from_path(name, path)
    e = EXTENDERS[name]

    return e


def extender(name: str):
    def decorator(fn: Callable):
        EXTENDERS[name] = fn
        return fn

    return decorator
