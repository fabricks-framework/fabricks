"""Unit tests: validate integration runtime YAML configs against Pydantic models."""

from pathlib import Path
from typing import List

import pytest
import yaml
from pydantic import TypeAdapter

from fabricks.models.job_schema import BronzeJobWrapper, GoldJobWrapper, SilverJobWrapper
from fabricks.models.runtime import RuntimeConf

RUNTIME = Path(__file__).parent.parent / "integration/runtime"

_bronze_ta = TypeAdapter(List[BronzeJobWrapper])
_silver_ta = TypeAdapter(List[SilverJobWrapper])
_gold_ta = TypeAdapter(List[GoldJobWrapper])


def _configs(path: Path) -> list[tuple[str, list]]:
    return [(f.stem, yaml.safe_load(f.read_text())) for f in sorted(path.glob("**/_config.*.yml"))]


_bronze = _configs(RUNTIME / "bronze")
_silver = _configs(RUNTIME / "silver")
_gold = _configs(RUNTIME / "gold") + _configs(RUNTIME / "semantic")


@pytest.mark.parametrize("name,data", _bronze, ids=[n for n, _ in _bronze])
def test_bronze_job_config(name: str, data: list) -> None:
    _bronze_ta.validate_python(data)


@pytest.mark.parametrize("name,data", _silver, ids=[n for n, _ in _silver])
def test_silver_job_config(name: str, data: list) -> None:
    _silver_ta.validate_python(data)


@pytest.mark.parametrize("name,data", _gold, ids=[n for n, _ in _gold])
def test_gold_job_config(name: str, data: list) -> None:
    _gold_ta.validate_python(data)


def test_runtime_conf() -> None:
    raw = yaml.safe_load((RUNTIME / "fabricks/conf.fabricks.yml").read_text())
    RuntimeConf.model_validate(raw[0]["conf"])
