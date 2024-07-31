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
    """
    Fixes the given SQL query by parsing it using the 'fabricks' dialect,
    transpiling it, and returning the fixed SQL query.

    Args:
        sql (str): The SQL query to be fixed.

    Returns:
        str: The fixed SQL query.
    """
    sql = parse_one(sql, dialect="fabricks").sql()
    sql = transpile(
        sql,
        identify=True,
        pretty=True,
        normalize=False,
        normalize_functions="lower",
        write="fabricks",
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
