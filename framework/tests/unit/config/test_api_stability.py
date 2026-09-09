"""Locks down fabricks/api/'s public re-export surface -- the facade user
notebooks/pipelines import from (`from fabricks.api import get_job`), as
opposed to fabricks/core/ internals which are free to change shape.

EXPECTED_ALL is the contract: every name a module re-exports, kept in sync
by hand. A rename, removal, or forgotten export in any fabricks/api/*.py
file changes __all__ and fails here, forcing a deliberate update to this
file instead of an silent, unreviewed break for anyone importing fabricks.api.

fabricks/api/notebooks/ is deliberately excluded: those files are Databricks
notebook scripts referenced by path (see databricks.yml's notebook_task
entries), not importable library modules -- they have no __all__ and run
dbutils widget/exit side effects at import time.
"""

import importlib
from pathlib import Path

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[3]
_API_ROOT = _FRAMEWORK_ROOT / "fabricks" / "api"

EXPECTED_ALL = {
    "fabricks.api": ["Deploy", "get_job", "get_jobs", "get_step", "init_spark_session"],
    "fabricks.api.cdc": ["CDC", "NoCDC", "SCD0", "SCD1", "SCD2"],
    "fabricks.api.cdc.nocdc": ["NoCDC"],
    "fabricks.api.cdc.scd0": ["SCD0"],
    "fabricks.api.cdc.scd1": ["SCD1"],
    "fabricks.api.cdc.scd2": ["SCD2"],
    "fabricks.api.context": [
        "BRONZE",
        "BRONZES",
        "CONF_RUNTIME",
        "DBUTILS",
        "GOLD",
        "GOLDS",
        "SILVER",
        "SILVERS",
        "SPARK",
        "STEPS",
        "Bronzes",
        "Golds",
        "Silvers",
        "init_spark_session",
        "pprint_runtime",
    ],
    "fabricks.api.core": ["BaseJob", "Bronze", "Gold", "Silver", "get_job", "get_jobs", "get_step"],
    "fabricks.api.deploy": ["Deploy"],
    "fabricks.api.exceptions": [
        "CheckError",
        "CheckWarning",
        "PostRunCheckException",
        "PostRunCheckWarning",
        "PreRunCheckException",
        "PreRunCheckWarning",
        "SkipRunCheckWarning",
    ],
    "fabricks.api.extenders": ["extender"],
    "fabricks.api.job_schema": [
        "get_job_schema",
        "job_schema",
        "job_schema_bronze",
        "job_schema_gold",
        "job_schema_silver",
        "print_job_schema",
    ],
    "fabricks.api.log": ["DEFAULT_LOGGER", "send_message_to_channel"],
    "fabricks.api.masks": ["register_all_masks", "register_mask"],
    "fabricks.api.metastore": ["Database", "Table", "View", "create_or_replace_view"],
    "fabricks.api.metastore.database": ["Database"],
    "fabricks.api.metastore.table": ["Table"],
    "fabricks.api.metastore.view": ["View", "create_or_replace_view"],
    "fabricks.api.parsers": ["BaseParser", "ParserOptions", "parser"],
    "fabricks.api.schedules": ["create_or_replace_view", "create_or_replace_views", "generate", "process", "terminate"],
    "fabricks.api.udfs": ["register_all_udfs", "register_udf", "udf"],
    "fabricks.api.utils": ["FileSharePath", "GitPath", "Path", "concat_dfs", "concat_ws", "find_upward", "run_in_parallel"],
    "fabricks.api.version": ["FABRICKS_VERSION"],
    "fabricks.api.views": ["create_or_replace_view", "create_or_replace_views"],
}


def _discover_api_modules() -> set[str]:
    modules = set()
    for path in _API_ROOT.rglob("*.py"):
        rel = path.relative_to(_API_ROOT)
        if rel.parts[0] == "notebooks":
            continue
        parts = rel.with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        modules.add(".".join(["fabricks", "api", *parts]))
    return modules


def test_no_undocumented_api_modules():
    # Fails if a new fabricks/api/*.py file is added (or one is removed)
    # without updating EXPECTED_ALL above.
    assert _discover_api_modules() == set(EXPECTED_ALL)


def test_api_all_matches_expected():
    for module_name, expected in EXPECTED_ALL.items():
        module = importlib.import_module(module_name)
        assert sorted(module.__all__) == sorted(expected), module_name


def test_api_all_names_are_importable():
    for module_name, expected in EXPECTED_ALL.items():
        module = importlib.import_module(module_name)
        for name in expected:
            assert hasattr(module, name), f"{module_name}.{name}"
