from importlib.util import module_from_spec, spec_from_file_location
from typing import Callable

from fabricks.context import PATH_EXTENDERS

EXTENDERS: dict[str, Callable] = {}


def get_extender(name: str) -> Callable:
    path = PATH_EXTENDERS.join(f"{name}.py")
    assert path.exists(), "no valid extender found in {path.string}"

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
