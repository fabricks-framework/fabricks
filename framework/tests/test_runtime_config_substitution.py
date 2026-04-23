from pathlib import Path

import pytest
import yaml

from fabricks.utils.runtime_config import prepare_runtime_conf_data


def test_prepare_runtime_conf_data_substitutes_all_fields() -> None:
    conf_data = {
        "name": "test",
        "options": {
            "workers": "$workers",
            "catalog": "$catalog",
            "retention_days": "$retention_days",
            "timeout_multiplier": "$timeout_multiplier",
            "feature_enabled": "$feature_enabled",
        },
        "path_options": {"storage": "abfss://fabricks@$storage/fabricks"},
        "variables": {
            "$workers": 8,
            "$catalog": "stg_dev_dwh",
            "$storage": "account.dfs.core.windows.net",
            "$retention_days": 14,
            "$timeout_multiplier": 1.5,
            "$feature_enabled": True,
        },
    }

    prepared = prepare_runtime_conf_data(conf_data=conf_data, config_path=Path("/tmp/conf.fabricks.yml"))

    assert prepared["options"]["workers"] == 8
    assert prepared["options"]["catalog"] == "stg_dev_dwh"
    assert prepared["options"]["retention_days"] == 14
    assert prepared["options"]["timeout_multiplier"] == 1.5
    assert prepared["options"]["feature_enabled"] is True
    assert prepared["path_options"]["storage"] == "abfss://fabricks@account.dfs.core.windows.net/fabricks"


def test_prepare_runtime_conf_data_merges_variables_file_with_override(tmp_path: Path) -> None:
    variables_file = tmp_path / "variables.dev.yml"
    variables_file.write_text(yaml.safe_dump({"$workers": 2, "$catalog": "stg_dev_dwh"}), encoding="utf-8")

    conf_data = {
        "name": "test",
        "options": {"workers": "$workers", "catalog": "$catalog"},
        "variables_file": str(variables_file),
        "variables": {"$workers": 8},
    }

    prepared = prepare_runtime_conf_data(conf_data=conf_data, config_path=tmp_path / "conf.fabricks.yml")

    assert prepared["options"]["workers"] == 2
    assert prepared["options"]["catalog"] == "stg_dev_dwh"
    assert "variables_file" not in prepared


def test_prepare_runtime_conf_data_external_variables_file_takes_precedence(tmp_path: Path) -> None:
    external_variables_file = tmp_path / "variables.prd.yml"
    external_variables_file.write_text(yaml.safe_dump({"$workers": 16}), encoding="utf-8")

    conf_data = {
        "name": "test",
        "options": {"workers": "$workers"},
        "variables_file": "ignored.yml",
        "variables": {"$workers": 4},
    }

    prepared = prepare_runtime_conf_data(
        conf_data=conf_data,
        config_path=tmp_path / "conf.fabricks.yml",
        external_variables_file=str(external_variables_file),
    )

    assert prepared["options"]["workers"] == 16


def test_prepare_runtime_conf_data_supports_escaped_variable_keys() -> None:
    conf_data = {
        "name": "test",
        "options": {"workers": "$workers"},
        "variables": {"\\$workers": 6},
    }

    prepared = prepare_runtime_conf_data(conf_data=conf_data, config_path=Path("/tmp/conf.fabricks.yml"))

    assert prepared["options"]["workers"] == 6


def test_prepare_runtime_conf_data_raises_for_missing_variables_file(tmp_path: Path) -> None:
    config_path = tmp_path / "conf.fabricks.yml"
    expected_variables_path = tmp_path / "variables.dev.yml"
    conf_data = {
        "name": "test",
        "options": {"workers": "$workers"},
        "variables_file": "variables.dev.yml",
        "variables": {"$workers": 4},
    }

    with pytest.raises(
        FileNotFoundError,
        match=f"variables file '{expected_variables_path}' referenced by config '{config_path}' was not found",
    ):
        prepare_runtime_conf_data(
            conf_data=conf_data,
            config_path=config_path,
        )
