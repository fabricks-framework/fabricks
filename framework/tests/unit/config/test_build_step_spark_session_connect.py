"""build_step_spark_session (fabricks/core/jobs/base/resolver.py) derives a
per-step session via `SPARK.newSession()`. Under Spark Connect, `newSession`
isn't a real method -- pyspark's Connect SparkSession raises
PySparkAttributeError for it via __getattr__ (pyspark/sql/connect/
session.py), instead of the JVM-backed classic session's real newSession().
_derive_session falls back to the parent session (pre-#214 behavior) rather
than crashing. See https://github.com/fabricks-framework/fabricks/issues/215.
"""

from unittest.mock import MagicMock

from pyspark.errors.exceptions.base import PySparkAttributeError
import pytest

from fabricks.core.jobs.base import resolver
from fabricks.models import SparkOptions


class _ConnectLikeSparkSession(MagicMock):
    """Mimics pyspark.sql.connect.session.SparkSession.__getattr__, which
    raises PySparkAttributeError for newSession instead of defining it.
    """

    def __getattr__(self, name: str):
        if name == "newSession":
            raise PySparkAttributeError(
                errorClass="JVM_ATTRIBUTE_NOT_SUPPORTED", messageParameters={"attr_name": name}
            )
        return super().__getattr__(name)


def test_build_step_spark_session_under_spark_connect(monkeypatch):
    connect_spark = _ConnectLikeSparkSession(name="connect_spark_session")
    monkeypatch.setattr(resolver, "SPARK", connect_spark)
    monkeypatch.setattr(resolver, "_STEP_SESSIONS", {})

    session = resolver.build_step_spark_session("connect_test_step", SparkOptions(conf={"some.conf": "value"}))

    assert session is connect_spark, "falls back to the parent session when newSession() isn't supported"
    session.conf.set.assert_any_call("some.conf", "value")


def test_resolver_cache_is_left_unchanged_by_the_connect_test():
    before = dict(resolver._STEP_SESSIONS)
    with pytest.MonkeyPatch.context() as mp:
        test_build_step_spark_session_under_spark_connect(mp)
    assert before == resolver._STEP_SESSIONS
