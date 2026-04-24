"""Utility functions for config loading and resolution."""

import json
import os
import pathlib
import sys
from pathlib import Path as PathLibPath
from typing import Any

from pydantic.fields import FieldInfo
from pydantic_settings import PydanticBaseSettingsSource

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore


class HierarchicalFileSettingsSource(PydanticBaseSettingsSource):
    """Custom settings source for hierarchical file configuration."""

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        return super().get_field_value(field, field_name)

    def __call__(self):
        """Load settings from hierarchical file search."""
        data = self._load_hierarchical_file()
        return data

    def _load_hierarchical_file(self):
        """Search up directory hierarchy for configuration files."""

        def pyproject_settings(base: PathLibPath):
            pyproject_path = base / "pyproject.toml"
            if pyproject_path.exists():
                with open(pyproject_path, "rb") as f:
                    data = tomllib.load(f)

                data = data.get("tool", {}).get("fabricks", {})
                data["base"] = str(base)
                data["path_to_config"] = str(pyproject_path)
                return data

            return None

        def json_settings(base: PathLibPath):
            json_path = base / "fabricksconfig.json"
            if json_path.exists():
                with open(json_path, "r") as f:
                    data = json.load(f)

                data["base"] = str(base)
                data["path_to_config"] = str(json_path)
                return data

            return None

        path = pathlib.Path(os.getcwd())
        data = None

        while not data:
            data = json_settings(path)
            if data:
                break

            data = pyproject_settings(path)
            if data:
                break

            if path == path.parent:
                break

            path = path.parent

        return data or {}
