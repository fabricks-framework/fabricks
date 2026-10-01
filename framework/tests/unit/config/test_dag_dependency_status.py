"""DagGenerator.get_dependencies() must emit every 'dependencies'-partition row with Status = 'pending', so
DagProcessor.send() can filter its readiness query on it. SPARK is a MagicMock here: only the query text is checked."""

from fabricks.context import SPARK
from fabricks.core.dags import generator
from fabricks.core.dags.generator import DagGenerator


def test_get_dependencies_selects_pending_status(monkeypatch):
    SPARK.sql.reset_mock()
    # the real pyspark `lit()` needs a SparkContext, which this tier does not have
    monkeypatch.setattr(generator, "lit", lambda _value: None)

    dag_generator = DagGenerator(schedule="unit-test")
    dag_generator.get_dependencies(job_df=SPARK.sql.return_value)

    query = SPARK.sql.call_args.args[0]
    assert "'pending' as Status" in query
