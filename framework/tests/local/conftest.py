"""Conftest for local tests - real Spark+Delta, no Databricks. Applies 'local' marker.

IMPORTANT: the Delta-configured SparkSession below is built here, at this
file's *module import time* — not lazily inside a fixture body. pytest loads
a directory's conftest.py before collecting any test module in it, which is
what makes this win the race against fabricks.context's eager, non-Delta
SPARK singleton (see Task 7's "Why" note in the plan). Do not move this into
`local_spark`'s function body — that reintroduces the race.

Do not mix `tests/local` with `tests/unit`/`tests/databricks` in the same
pytest invocation — `fabricks.context.runtime` resolves CONF_RUNTIME once per
process, and the first import wins.
"""

import os
from pathlib import Path
import shutil

import pytest

# parents[2]: local -> tests -> framework. Deliberately NOT parents[3]/"framework"
# (the repo root then re-descending) - that only happens to work on the host.
# docker-compose.yml bind-mounts the "framework" dir itself to /workspace, so
# inside the container parents[3] lands on the container's filesystem root and
# FABRICKS_BASE would resolve to the nonexistent "/framework".
_FRAMEWORK_ROOT = Path(__file__).resolve().parents[2]
_LOCAL_STORAGE = _FRAMEWORK_ROOT / "tests" / "local" / ".storage"

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_CONFIG"] = "tests/local/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"

shutil.rmtree(_LOCAL_STORAGE, ignore_errors=True)

# get_spark()'s docker branch (enableHiveSupport(), no spark.sql.warehouse.dir
# override - Task 4, out of scope here) defaults Hive's embedded Derby
# metastore and warehouse to CWD (/workspace in the container, bind-mounted to
# this framework/ dir on the host - see docker-compose.yml). Without this,
# table registration from a previous `podman compose run` leaks into the next
# one (each `run --rm` is a fresh container, but /workspace is not).
for _leftover in ("metastore_db", "spark-warehouse"):
    shutil.rmtree(_FRAMEWORK_ROOT / _leftover, ignore_errors=True)
(_FRAMEWORK_ROOT / "derby.log").unlink(missing_ok=True)

from pyspark.sql import SparkSession  # noqa: E402

from fabricks.utils.spark import get_spark  # noqa: E402 - must follow the env-var setup above

_SPARK: SparkSession = get_spark()

from fabricks.metastore.database import Database  # noqa: E402 - must follow _SPARK's construction above

for _db_name in ("bronze", "silver", "gold", "expected"):
    Database(_db_name, spark=_SPARK).create()


def pytest_collection_modifyitems(items):
    """Automatically add 'local' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        try:
            if Path(item.fspath).is_relative_to(root):
                item.add_marker(pytest.mark.local)
        except (ValueError, AttributeError):
            if "local" in str(item.fspath):
                item.add_marker(pytest.mark.local)


@pytest.fixture(scope="session")
def local_spark():
    yield _SPARK
    _SPARK.stop()
