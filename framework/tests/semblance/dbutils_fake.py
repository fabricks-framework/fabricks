"""Strict fake of the `dbutils` surface Fabricks calls. Anything not modelled raises AttributeError."""
# ruff: noqa: N802, N803  (method and parameter names mirror the SDK dbutils stub, which is camelCase)

from collections import namedtuple
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
import shutil
from typing import Any, NamedTuple

from tests.semblance.stub import conforms_to


class NotebookCall(NamedTuple):
    path: str
    timeout_seconds: int
    arguments: Mapping[str, str]


class NotebookExit(Exception):  # noqa: N818 - mirrors the runtime's exit signal, not an error
    def __init__(self, value: str) -> None:
        super().__init__(value)
        self.value = value


class _FileInfo(namedtuple("_FileInfo", ["path", "name", "size", "modificationTime"])):
    def isDir(self) -> bool:  # the runtime's FileInfo has isDir(); the SDK stub's namedtuple does not
        return self.name.endswith("/")


_Scope = namedtuple("_Scope", ["name"])


@dataclass
class DbutilsState:
    fs_root: Path
    widgets: dict[str, str] = field(default_factory=dict)
    secrets: dict[tuple[str, str], str] = field(default_factory=dict)
    task_values: dict[str, Any] = field(default_factory=dict)
    notebook_calls: list[NotebookCall] = field(default_factory=list)
    # (path or None for any, returned status | callable(NotebookCall) -> status); latest registration wins
    notebook_results: list[tuple[str | None, Any]] = field(default_factory=list)


class _Widgets:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    @conforms_to("widgets.get")
    def get(self, name):
        if name not in self._values:
            raise ValueError(f"widget {name!r} is not defined")
        return self._values[name]

    @conforms_to("widgets.text")
    def text(self, name, defaultValue, label=None):
        self._values.setdefault(name, defaultValue)  # a value passed in by the job wins, as on Databricks


class _Secrets:
    def __init__(self, values: dict[tuple[str, str], str]) -> None:
        self._values = values

    @conforms_to("secrets.get")
    def get(self, scope, key):
        try:
            return self._values[(scope, key)]
        except KeyError:
            raise ValueError(f"secret {scope}/{key} not found") from None

    @conforms_to("secrets.listScopes")
    def listScopes(self):
        return [_Scope(name) for name in sorted({scope for scope, _ in self._values})]


class _Fs:
    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def _resolve(self, path: str) -> Path:
        p = Path(path).resolve()
        if not p.is_relative_to(self._root):
            raise PermissionError(f"{path!r} is outside the semblance fs_root {self._root}")
        return p

    def _info(self, p: Path) -> _FileInfo:
        suffix = "/" if p.is_dir() else ""
        stat = p.stat()
        return _FileInfo(f"{p}{suffix}", f"{p.name}{suffix}", stat.st_size, int(stat.st_mtime * 1000))

    @conforms_to("fs.ls")
    def ls(self, path):
        p = self._resolve(path)
        if not p.exists():
            raise FileNotFoundError(path)
        return [self._info(p)] if p.is_file() else [self._info(c) for c in sorted(p.iterdir())]

    @conforms_to("fs.rm")
    def rm(self, dir, recurse=False):
        p = self._resolve(dir)
        if not p.exists():
            return False
        if p.is_dir():
            if not recurse and any(p.iterdir()):
                raise OSError(f"{dir} is a non-empty directory; pass recurse=True")
            shutil.rmtree(p)
        else:
            p.unlink()
        return True


class _Notebook:
    def __init__(self, state: DbutilsState) -> None:
        self._state = state

    @conforms_to("notebook.run")
    def run(self, path, timeout_seconds, arguments):
        call = NotebookCall(path, timeout_seconds, dict(arguments))
        self._state.notebook_calls.append(call)
        for match, returns in reversed(self._state.notebook_results):
            if match is None or match == path:
                return returns(call) if callable(returns) else returns
        raise AssertionError(
            f"unexpected dbutils.notebook.run({path!r}, ...): register it with semblance.on_notebook_run()"
        )

    @conforms_to("notebook.exit")
    def exit(self, value):
        raise NotebookExit(value)


class _TaskValues:
    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    @conforms_to("jobs.taskValues.get")
    def get(self, taskKey, key, default=None, debugValue=None):
        # taskKey is ignored: a local run has a single task. Unset outside a job raises TypeError,
        # which is exactly what schedules/dags.py and dags/run.py catch before falling back to widgets.
        if key in self._values:
            return self._values[key]
        if debugValue is not None:
            return debugValue
        if default is not None:
            return default
        raise TypeError("Must pass debugValue when calling get outside of a job context. debugValue cannot be None.")

    @conforms_to("jobs.taskValues.set")
    def set(self, key, value):
        self._values[key] = value


class _Jobs:
    def __init__(self, values: dict[str, Any]) -> None:
        self.taskValues = _TaskValues(values)


class FakeDbutils:
    def __init__(self, state: DbutilsState) -> None:
        self.widgets = _Widgets(state.widgets)
        self.secrets = _Secrets(state.secrets)
        self.fs = _Fs(state.fs_root)
        self.notebook = _Notebook(state)
        self.jobs = _Jobs(state.task_values)
