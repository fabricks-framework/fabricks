"""Real Apache Spark and Delta bootstrap for this test tier."""

import os
from pathlib import Path
import shutil

import pytest

from tests.tier_policy import activate_tier

activate_tier("apache")

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[3]
_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
_LOCAL_STORAGE = _FRAMEWORK_ROOT / "tests" / "spark" / "apache" / ".storage"
_EXPECTED_CACHE = _FRAMEWORK_ROOT / "tests" / "spark" / "apache" / ".expected-cache"
_WORKER_ROOT = _FRAMEWORK_ROOT / "tests" / "spark" / "apache" / ".worker_cwd" / _WORKER
_WORKER_ROOT.mkdir(parents=True, exist_ok=True)
_VARIABLES_FILE = _WORKER_ROOT / "variables.yml"
_VARIABLES_FILE.write_text(f"$apache_storage: {_LOCAL_STORAGE}\n")

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_RUNTIME"] = "tests/spark/apache/runtime"
os.environ["FABRICKS_TEST_SPARK_DEFAULT_PARALLELISM"] = "2"
os.environ["FABRICKS_TEST_DISPOSABLE_STORAGE"] = str(_LOCAL_STORAGE)
os.environ["FABRICKS_TEST_EXPECTED_CACHE"] = str(_EXPECTED_CACHE)
os.environ["FABRICKS_CONFIG"] = "tests/spark/apache/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_VARIABLE"] = str(_VARIABLES_FILE)
os.environ["FABRICKS_ENVIRONMENT"] = "docker"
os.environ["FABRICKS_IS_DEBUGMODE"] = "FALSE"
os.environ["FABRICKS_LOGLEVEL"] = "WARNING"
os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "TRUE"

shutil.rmtree(_LOCAL_STORAGE, ignore_errors=True)
os.environ["_JAVA_OPTIONS"] = f"-Dderby.system.home={_WORKER_ROOT}"
os.environ["FABRICKS_TEST_WAREHOUSE_DIR"] = str(_WORKER_ROOT / "spark-warehouse")
for _leftover in ("metastore_db", "spark-warehouse"):
    shutil.rmtree(_WORKER_ROOT / _leftover, ignore_errors=True)
(_WORKER_ROOT / "derby.log").unlink(missing_ok=True)

from pyspark.sql import SparkSession  # noqa: E402

from fabricks.utils.spark import get_spark  # noqa: E402

_SPARK: SparkSession = get_spark()
_SPARK.sql("set spark.sql.sources.default = delta")
_SPARK.conf.set("spark.sql.shuffle.partitions", "2")
_SPARK.conf.set("spark.databricks.delta.snapshotPartitions", "2")
_SPARK.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "false")

for _database in ("bronze", "silver", "gold", "semantic", "expected", "cdc", "fabricks"):
    _SPARK.sql(f"create database if not exists {_database}")
@pytest.fixture(scope="session")
def local_spark():
    yield _SPARK
    _SPARK.stop()
