from sqlglot import exp, parse, parse_one, transpile
from sqlglot.dialects.databricks import Databricks


class Fabricks(Databricks):
    class Generator(Databricks.Generator):
        EXPRESSIONS_WITHOUT_NESTED_CTES = {
            exp.Insert,
            exp.Union,
        }


def fix(sql: str, keep_comments: bool = True):
    parts = transpile(
        sql,
        "fabricks",
        identify=True,
        pretty=True,
        normalize=False,
        normalize_functions="lower",
        leading_comma=True,
        max_text_width=119,
        comments=keep_comments,
    )
    sql = ";\n".join(parts)
    return sql


def is_global_temp_view(sql: str):
    tables = parse_one_fabricks(sql).find_all(exp.Table)
    for t in tables:
        return "global_temp" in str(t)


def get_global_temp_view(sql: str) -> str | None:
    tables = parse_one_fabricks(sql).find_all(exp.Table)
    for t in tables:
        if "global_temp" in str(t):
            return str(t)


def parse_one_fabricks(sql: str) -> exp.Expression:
    return parse_one(sql, dialect="fabricks")


def parse_fabricks(sql: str) -> list[exp.Expression | None]:
    return parse(sql, dialect="fabricks")


def get_tables(sql: str, allowed_databases: list[str] | None = None) -> list[str]:
    tables = []
    parts = [p for p in parse_fabricks(sql) if p is not None]

    for part in parts:
        for table in part.find_all(exp.Table):
            if len(table.db) > 0:  # exclude CTEs
                if allowed_databases:
                    if table.db not in allowed_databases:
                        continue

                tables.append(f"{table.db}.{table.name}")

    # Remove duplicates
    return list(set(tables))


def parse_script(sql: str) -> list[str]:
    parts = [p for p in parse_fabricks(sql) if p is not None]
    return [p.sql(dialect="fabricks") for p in parts]


def parse_script_expressions(sql: str) -> list[exp.Expression]:
    parts = [p for p in parse_fabricks(sql) if p is not None]
    return parts
