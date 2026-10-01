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


def test_build_step_spark_session_derives_an_isolated_session_when_new_session_works(monkeypatch):
    # Control for the Connect fallback: it must not be taken when newSession() is supported (#214 isolation).
    parent = MagicMock(name="parent_spark_session")
    derived = parent.newSession.return_value
    monkeypatch.setattr(resolver, "SPARK", parent)
    monkeypatch.setattr(resolver, "_STEP_SESSIONS", {})

    session = resolver.build_step_spark_session("classic_step", SparkOptions(conf={"some.conf": "value"}))

    assert session is derived
    derived.conf.set.assert_any_call("some.conf", "value")
    parent.conf.set.assert_not_called()


def test_build_step_spark_session_is_cached_per_step(monkeypatch):
    parent = MagicMock(name="parent_spark_session")
    monkeypatch.setattr(resolver, "SPARK", parent)
    monkeypatch.setattr(resolver, "_STEP_SESSIONS", {})
    options = SparkOptions(conf={"some.conf": "value"})

    first = resolver.build_step_spark_session("cached_step", options)
    second = resolver.build_step_spark_session("cached_step", options)

    assert first is second
    parent.newSession.assert_called_once()


def test_build_step_spark_session_without_options_returns_the_runtime_session(monkeypatch):
    parent = MagicMock(name="parent_spark_session")
    monkeypatch.setattr(resolver, "SPARK", parent)

    assert resolver.build_step_spark_session("plain_step", None) is parent
    parent.newSession.assert_not_called()
