from typing import List, Optional

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


def parse(sql: str) -> exp.Expression:
    return parse_one(sql, dialect="fabricks")


def get_tables(sql: str, allowed_databases: Optional[List[str]] = None) -> List[str]:
    tables = set()
    for table in parse(sql).find_all(exp.Table):
        if len(table.db) > 0:  # exclude CTEs
            if allowed_databases:
                if table.db not in allowed_databases:
                    continue
            tables.add(f"{table.db}.{table.name}")
    tables = list(tables)
    return tables
