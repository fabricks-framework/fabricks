from typing import Optional

from sqlglot import exp, parse_one, transpile
from sqlglot.dialects.databricks import Databricks


class Fabricks(Databricks):
    class Generator(Databricks.Generator):
        EXPRESSIONS_WITHOUT_NESTED_CTES = {
            exp.Insert,
            exp.Union,
        }


def fix(sql: str):
    sql = transpile(
        sql,
        "fabricks",
        identify=True,
        pretty=True,
        normalize=False,
        normalize_functions="lower",
        leading_comma=True,
        max_text_width=119,
    )[0]
    return sql


def is_global_temp_view(sql: str):
    tables = parse_one(sql, dialect="fabricks").find_all(exp.Table)
    for t in tables:
        return "global_temp" in str(t)


def get_global_temp_view(sql: str) -> Optional[str]:
    tables = parse_one(sql, dialect="fabricks").find_all(exp.Table)
    for t in tables:
        if "global_temp" in str(t):
            return str(t)
