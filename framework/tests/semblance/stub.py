"""Bind fake `dbutils` calls against the SDK's real signatures.

The stub is loaded by file path, not `import databricks.sdk.runtime.dbutils_stub`: the config tier
replaces `databricks.sdk.runtime` in sys.modules with a non-package mock, and the real package's
__init__ tries to authenticate against a workspace. The stub is typing-only, so it can lag the real
runtime; the pinned databricks-sdk is the reference.
"""

import functools
import importlib.util
import inspect
from pathlib import Path
from types import ModuleType


@functools.cache
def _stub() -> ModuleType:
    spec = importlib.util.find_spec("databricks.sdk")
    assert spec
    assert spec.submodule_search_locations
    path = Path(spec.submodule_search_locations[0]) / "runtime" / "dbutils_stub.py"
    module_spec = importlib.util.spec_from_file_location("_semblance_dbutils_stub", path)
    assert module_spec
    assert module_spec.loader
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def stub_signature(path: str) -> inspect.Signature:
    """Signature of e.g. "widgets.get" or "jobs.taskValues.set" on the SDK's dbutils stub."""
    obj = _stub().dbutils
    for part in path.split("."):
        obj = getattr(obj, part)
    return inspect.signature(obj)


def conforms_to(path: str):
    """Decorate a fake method so calls are bound against the stub signature first (TypeError on mismatch)."""

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            stub_signature(path).bind(*args, **kwargs)
            return fn(self, *args, **kwargs)

        wrapper.stub_path = path
        return wrapper

    return decorate
