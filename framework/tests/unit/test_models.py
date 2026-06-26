"""Unit tests for Fabricks Pydantic models using real fixture configs."""

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from fabricks.models.config.models import ConfigOptions
from fabricks.models.job import JobConfBronze, JobConfGold, JobConfSilver
from fabricks.models.runtime.models import RuntimeConf

FIXTURES = Path(__file__).parent.parent / "fixtures/runtime"


def _load_jobs(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [entry["job"] for entry in yaml.safe_load(f)]


def _load_conf(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)[0]["conf"]


# ---   ---


def test_runtime_conf_loads_fixture():
    conf = RuntimeConf.model_validate(_load_conf(FIXTURES / "fabricks/conf.fabricks.yml"))
    assert conf.name == "test"
    assert conf.options.secret_scope == "bmskv"
    assert conf.options.workers == 8
    assert conf.options.retention_days == 7

    assert conf.bronze
    assert len(conf.bronze) == 1
    assert conf.silver
    assert len(conf.silver) == 1
    assert conf.gold
    assert len(conf.gold) == 3


def test_runtime_conf_missing_required(minimal_runtime_config):
    del minimal_runtime_config["options"]["secret_scope"]
    with pytest.raises(ValidationError):
        RuntimeConf.model_validate(minimal_runtime_config)


# --- Bronze ---


def test_bronze_all_jobs_parse():
    jobs = _load_jobs(FIXTURES / "bronze/_config.kings.yml")
    confs = [JobConfBronze.model_validate(j) for j in jobs]
    assert all(c.step == "bronze" for c in confs)
    assert all(c.options.mode == "append" for c in confs)


def test_bronze_uri_and_parser():
    jobs = _load_jobs(FIXTURES / "bronze/_config.kings.yml")
    conf = JobConfBronze.model_validate(jobs[0])
    assert conf.options.uri.startswith("abfss://")
    assert conf.options.parser == "monarch"


# --- Silver ---


def test_silver_all_jobs_parse():
    jobs = _load_jobs(FIXTURES / "silver/_config.monarchs.yml")
    confs = [JobConfSilver.model_validate(j) for j in jobs]
    assert all(c.step == "silver" for c in confs)


def test_silver_cdc_modes():
    jobs = _load_jobs(FIXTURES / "silver/_config.monarchs.yml")
    confs = [JobConfSilver.model_validate(j) for j in jobs]
    cdcs = {c.options.change_data_capture for c in confs}
    assert {"scd1", "scd2", "nocdc"} <= cdcs


# --- Gold ---


def test_gold_all_fact_jobs_parse():
    jobs = _load_jobs(FIXTURES / "gold/gold/fact/_config.fact.yml")
    confs = [JobConfGold.model_validate(j) for j in jobs]
    assert all(c.step == "gold" for c in confs)


def test_gold_scd1_jobs_parse():
    jobs = _load_jobs(FIXTURES / "gold/gold/scd1/_config.scd1.yml")
    confs = [JobConfGold.model_validate(j) for j in jobs]
    assert all(c.options.change_data_capture == "scd1" for c in confs)


def test_gold_table_options_parsed():
    jobs = _load_jobs(FIXTURES / "gold/gold/fact/_config.fact.yml")
    option_job = next(j for j in jobs if j["item"] == "option")
    conf = JobConfGold.model_validate(option_job)
    assert conf.table_options is not None
    assert conf.table_options.liquid_clustering is True
    assert conf.table_options.cluster_by == ["monarch"]


# --- ConfigOptions (fabricksconfig.json) ---


def test_runtime_resolves_above_config_dir():
    # fabricksconfig.json sits in tests/unit/, runtime is one level up in tests/fixtures/runtime
    base = Path(__file__).parent
    conf = ConfigOptions(base=base.as_posix(), runtime="../fixtures/runtime", notebooks="./notebooks")
    resolved = conf._resolve_paths()
    assert resolved.runtime.string == (base.parent / "fixtures/runtime").as_posix()
