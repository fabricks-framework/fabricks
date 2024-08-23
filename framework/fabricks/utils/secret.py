from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache

from databricks.sdk.runtime import dbutils, spark


@dataclass
class Secret:
    pass


@dataclass
class ApplicationRegistration(Secret):
    secret: str
    application_id: str
    directory_id: str


@dataclass
class AccessKey(Secret):
    key: str


_scopes = None


@lru_cache(maxsize=None)
def _get_secret_from_secret_scope(secret_scope: str, name: str) -> str:
    global _scopes
    if not _scopes or secret_scope not in _scopes:  # we get the scopes only once, unless you search for something new
        _scopes = [s.name for s in dbutils.secrets.listScopes()]
    assert secret_scope in _scopes, "scope {secret_scope} not found"
    return dbutils.secrets.get(scope=secret_scope, key=name)


def get_secret_from_secret_scope(secret_scope: str, name: str) -> Secret:
    secret = _get_secret_from_secret_scope(secret_scope=secret_scope, name=name)
    if name.endswith("application-registration"):
        s = json.loads(secret)
        assert s.get("secret"), f"no secret found in {name}"
        assert s.get("application_id"), f"no application_id found in {name}"
        assert s.get("directory_id"), f"no directory_id found in {name}"
        return ApplicationRegistration(
            secret=s.get("secret"),
            application_id=s.get("application_id"),
            directory_id=s.get("directory_id"),
        )
    elif name.endswith("access-key"):
        return AccessKey(key=secret)
    else:
        raise ValueError(f"{name} is not valid")


def _add_secret_to_spark(key: str, value: str):
    spark.conf.set(key, value)
    # needed for check (invalid configuration value detected for fs.azure.account.key)
    spark._jsc.hadoopConfiguration().set(key, value)  # type: ignore


def add_secret_to_spark(secret: Secret, uri: str):
    if isinstance(secret, ApplicationRegistration):
        _add_secret_to_spark(f"fs.azure.account.auth.type.{uri}", "OAuth")
        _add_secret_to_spark(
            f"fs.azure.account.oauth.provider.type.{uri}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        _add_secret_to_spark(
            f"fs.azure.account.oauth2.client.id.{uri}",
            secret.application_id,
        )
        _add_secret_to_spark(
            f"fs.azure.account.oauth2.client.secret.{uri}",
            secret.secret,
        )
        _add_secret_to_spark(
            f"fs.azure.account.oauth2.client.endpoint.{uri}",
            f"https://login.microsoftonline.com/{secret.directory_id}/oauth2/token",
        )
    elif isinstance(secret, AccessKey):
        _add_secret_to_spark(f"fs.azure.account.key.{uri}", secret.key)
    else:
        raise ValueError("secret is not valid")
