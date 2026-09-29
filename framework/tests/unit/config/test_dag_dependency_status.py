"""DagGenerator.get_dependencies() (fabricks/core/dags/generator.py) must
emit every 'dependencies'-partition row with Status = 'pending' at
creation, so processor.py's send() can filter its readiness query on it.

SPARK is a session-wide MagicMock in this tier (see conftest.py) -- real
SQL execution/columns aren't available here, only the query text and call
shape, which is exactly what this one-line addition needs proving.
"""

from fabricks.context import SPARK
from fabricks.core.dags import generator
from fabricks.core.dags.generator import DagGenerator


def test_get_dependencies_selects_pending_status(monkeypatch):
    SPARK.sql.reset_mock()
    # get_dependencies() also calls the real pyspark `lit()` on the mocked
    # df afterwards (no real SparkContext in this tier) -- irrelevant to
    # this test, which only cares about the SQL text SPARK.sql() was
    # called with.
    monkeypatch.setattr(generator, "lit", lambda _value: None)

    dag_generator = DagGenerator(schedule="unit-test")
    dag_generator.get_dependencies(job_df=SPARK.sql.return_value)

    query = SPARK.sql.call_args.args[0]
    assert "'pending' as Status" in query
