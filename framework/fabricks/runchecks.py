"""Validate a Fabricks runtime repo's job YAML against its own SQL/notebook
files, without needing a Databricks connection.

Checks, across every `config.*.yml` / `_config.*.yml` file found by walking
the current directory:

- no two jobs declare the same table (`step.topic_item`)
- every `parents:` entry resolves to a table declared somewhere in the repo
- the `.sql`/notebook file a job needs exists (see "Passthrough steps" below)
- every notebook referenced by `invoker_options` exists
- every SQL file referenced by `check_options` (`pre_run`/`post_run`/`skip`)
  exists and parses as a valid query (via sqlglot, Databricks dialect)
- a job's own `.sql` file parses too -- as a single query, unless
  `options.script: true`, in which case it's checked as a multi-statement
  script instead (no single-query requirement)
- a `_config.<topic>.yml` file's own job topics agree with its filename
- no circular dependency between jobs (see "Dependency graph" below)
- a job without declared `parents:` has SQL referencing a table in a
  fabricks-managed database (its database matches a step name declared
  somewhere in the repo) that doesn't actually exist -- likely a typo,
  and an error since we know the complete set of fabricks-managed tables.
  A reference to a database that isn't a declared step (an external/raw
  source, another catalog, ...) can't be resolved the same way -- printed
  as a warning instead, since it might be entirely legitimate

`parents:` is an override: when a job sets it, Fabricks uses exactly that
list. When a job leaves it unset, Fabricks deducts the dependency from the
job's own SQL/notebook instead. The two are alternative ways to say the same
thing, not a pair that must agree -- this validator does not, and should
not, check a job's SQL against its own declared `parents:`.

Dependency graph: for each job, an edge is `parents:` if declared, else the
tables referenced in its own SQL file (parsed with sqlglot, ignoring CTEs
and non-Fabricks-managed names). This mirrors the override-vs-deduct rule
above; it does not replicate every step kind's exact deduction fallback
(e.g. a silver job's implicit same-topic-item parent when both `parents:`
and its own SQL are absent), so a cycle that only exists through such an
implicit edge won't be caught -- but any cycle formed by declared `parents:`
or by an explicit SQL reference will be.

Passthrough steps: a job in `mode: invoke`/`register`, or with `options.table`
set, never needs its own `.sql` file -- Fabricks generates the query itself.
Beyond that, bronze and silver jobs never author SQL either (bronze reads
`uri` directly, silver is always an implicit `select * from {parent}`) --
only gold (and other gold-kind steps) ever has a `.sql` file. This is
auto-discovered the same way Fabricks itself finds its config: walk up
from the runtime path for `fabricksconfig.json` or a `pyproject.toml`
with `[tool.fabricks]`, read its `config` key for the main conf YAML, and
collect every step name under that YAML's `bronze:`/`silver:` lists. Pass
`--config <path>` to point at that conf YAML directly instead of walking
for it (also a fix if discovery finds the wrong file). Either way,
`--passthrough-step <name>` (repeatable) still works as a manual
addition on top, for edge cases discovery can't know about. Discovery
finding nothing at all prints a warning (not an error -- the rest of the
checks still run, just without the bronze/silver `.sql`-file exemption).

Usage (from a runtime repo root):
    python -m fabricks.runchecks
    python -m fabricks.runchecks path/to/runtime
    python -m fabricks.runchecks --config fabricks/conf.standard.yml
    python -m fabricks.runchecks --passthrough-step raw --passthrough-step staging
    python -m fabricks.runchecks --verbose  # + a per-step jobs/topics/sql-files table

Deliberately does not import fabricks.api (which requires a resolved
runtime context / Spark session) -- this only needs pyyaml and sqlglot.
"""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys
import tomllib

import sqlglot
import sqlglot.expressions as exp
import yaml

from fabricks.utils.sqlglot import get_tables

_NONE_SENTINELS = {"none", "null", "0"}


def _find_conf_yaml(runtime: Path) -> Path | None:
    """Locates the runtime's main conf YAML the same way Fabricks itself
    does: walk up for `fabricksconfig.json` or a `pyproject.toml` with
    `[tool.fabricks]`, and read its `config` key."""
    path = runtime.resolve()

    while True:
        json_path = path / "fabricksconfig.json"
        toml_path = path / "pyproject.toml"
        settings = None
        if json_path.exists():
            settings = json.loads(json_path.read_text())
        elif toml_path.exists():
            settings = tomllib.loads(toml_path.read_text()).get("tool", {}).get("fabricks", {}) or None

        if settings and settings.get("config"):
            return path / settings["config"]
        if path.parent == path:
            return None
        path = path.parent


def _passthrough_steps_from_conf(conf_yaml: Path) -> frozenset[str]:
    """Steps that never author SQL: bronze and silver, per the runtime's
    main conf YAML (its `bronze:`/`silver:` step lists)."""
    try:
        conf = yaml.safe_load(conf_yaml.read_text(encoding="utf-8-sig"))
        steps = conf[0]["conf"]
        return frozenset(s["name"] for kind in ("bronze", "silver") for s in steps.get(kind, []))
    except (OSError, KeyError, IndexError, TypeError, yaml.YAMLError):
        return frozenset()


def _notebook_paths(root: Path, stem: str) -> list[Path]:
    return [root / f"{stem}{ext}" for ext in (".ipynb", ".py", "")]


def _check_sql(path: Path, is_script: bool = False) -> bool:
    """Returns True if `path` has an error. `is_script` (job `options.script:
    true`) means a multi-statement script is expected, not a single query."""
    sql = path.read_text(encoding="utf-8-sig")
    if sql.startswith(("%sql", "%s ql")):
        print(f"SQL in {path} starts with %sql")
        return True
    if sql.startswith("-- Databricks notebook source"):
        return False

    if is_script:
        try:
            sqlglot.parse(sql, dialect="databricks")
        except sqlglot.ParseError as e:
            print(f"SQL parse error in {path}: {e}")
            return True
        return False

    try:
        parsed = sqlglot.parse_one(sql, dialect="databricks")
    except sqlglot.ParseError as e:
        print(f"SQL parse error in {path}: {e}")
        return True
    if not isinstance(parsed, exp.Query):
        print(f"SQL in {path} is not a query: {type(parsed)}")
        return True
    return False


def _declared_parents(options: dict) -> list[str] | None:
    parents = options.get("parents")
    if not parents:
        return None
    if len(parents) == 1 and parents[0].lower() in _NONE_SENTINELS:
        return []
    return parents


def _find_cycle(graph: dict[str, set[str]]) -> list[str] | None:
    """Returns a table-name cycle (closed loop) if one exists, else None."""
    white, gray, black = 0, 1, 2
    color = dict.fromkeys(graph, white)
    path: list[str] = []

    def visit(node: str) -> list[str] | None:
        color[node] = gray
        path.append(node)
        for dep in graph.get(node, ()):
            if dep not in graph or color[dep] == black:
                continue
            if color[dep] == gray:
                return [*path[path.index(dep) :], dep]
            if cycle := visit(dep):
                return cycle
        path.pop()
        color[node] = black
        return None

    for node in graph:
        if color[node] == white and (cycle := visit(node)):
            return cycle
    return None


def _print_stats(jobs: dict[str, int], topics: dict[str, set[str]], sql_files: dict[str, int]) -> None:
    print(f"{'step':<20} {'jobs':>6} {'topics':>7} {'sql files':>10}")
    for step in sorted(jobs):
        print(f"{step:<20} {jobs[step]:>6} {len(topics[step]):>7} {sql_files[step]:>10}")


def check_config(runtime: Path, passthrough_steps: frozenset[str] = frozenset(), verbose: bool = False) -> bool:
    """Returns True if any error was found (and printed)."""
    errors = 0
    tables: set[str] = set()
    steps: set[str] = set()
    job2parent: dict[str, list[str]] = {}
    job_sql: dict[str, Path] = {}
    sqls_to_check: list[Path] = []
    script_sql: set[Path] = set()
    jobs_per_step: dict[str, int] = defaultdict(int)
    topics_per_step: dict[str, set[str]] = defaultdict(set)
    sql_files_per_step: dict[str, int] = defaultdict(int)

    config_files = [p for p in runtime.rglob("*.yml") if p.name.startswith(("config.", "_config."))]

    for path in config_files:
        root = path.parent
        file_topic = path.name.split(".")[1]

        with path.open(encoding="utf-8-sig") as f:
            yaml_data = yaml.load(f, yaml.SafeLoader)
        if yaml_data is None:
            continue

        topics = set()
        for entry in yaml_data:
            job = entry["job"]
            topics.add(job["topic"])
            options = job.get("options", {})
            table_name = f"{job['step']}.{job['topic']}_{job['item']}"
            steps.add(job["step"].lower())
            jobs_per_step[job["step"]] += 1
            topics_per_step[job["step"]].add(job["topic"])

            if table_name in tables:
                print(f"Duplicate table name found: {table_name} in {path}")
                errors += 1
            tables.add(table_name)

            if (parents := _declared_parents(options)) is not None:
                job2parent[table_name] = parents

            needs_own_file = (
                job["step"] not in passthrough_steps
                and options.get("mode") not in ("invoke", "register")
                and not options.get("table")
            )
            if needs_own_file:
                is_notebook = bool(options.get("notebook"))
                candidates = _notebook_paths(root, job["item"]) if is_notebook else [root / f"{job['item']}.sql"]
                found = next((c for c in candidates if c.exists()), None)
                if found is None:
                    print(f"File for {table_name} not found: {candidates[-1]}. Config incorrect or file missing?")
                    errors += 1
                elif not is_notebook:
                    sqls_to_check.append(found)
                    job_sql[table_name] = found
                    sql_files_per_step[job["step"]] += 1
                    if options.get("script"):
                        script_sql.add(found)

            for notebooks in (entry.get("invoker_options") or {}).values():
                for invoker in notebooks:
                    notebook = Path(invoker["notebook"])
                    if not any(p.exists() for p in _notebook_paths(notebook.parent, notebook.name)):
                        print(f"Notebook not found: {notebook} for {table_name}")
                        errors += 1

            for check in ("skip", "pre_run", "post_run"):
                if (entry.get("check_options") or {}).get(check):
                    fn = root / f"{job['item']}.{check}.sql"
                    if not fn.exists():
                        print(f"{check} check SQL not found: {fn} for {table_name}")
                        errors += 1
                    else:
                        sqls_to_check.append(fn)

        if len(topics) > 1:
            print(f"Multiple topics in {path}: {topics}")
            errors += 1
        elif len(topics) == 1 and not file_topic.startswith(topics.pop()):
            print(f"Topic mismatch in {path}: filename says {file_topic!r}")
            errors += 1

    for job, parents in job2parent.items():
        for parent in parents:
            if parent not in tables:
                print(f"Parent table not found: {parent} for {job}")
                errors += 1

    for sql_file in sqls_to_check:
        errors += _check_sql(sql_file, is_script=sql_file in script_sql)

    graph: dict[str, set[str]] = {}
    for table_name in tables:
        if table_name in job2parent:
            graph[table_name] = {p.lower() for p in job2parent[table_name]}
        elif table_name in job_sql:
            try:
                sql = job_sql[table_name].read_text(encoding="utf-8-sig")
                referenced = {t.replace("__current", "").lower() for t in get_tables(sql)}
            except sqlglot.ParseError:
                referenced = set()  # already reported above
            graph[table_name] = referenced - {table_name.lower()}

            for ref in sorted(graph[table_name]):
                db = ref.split(".", 1)[0]
                if db in steps:
                    if ref not in tables:
                        print(f"SQL for {table_name} references {ref} (no such job) -- typo?")
                        errors += 1
                else:
                    print(f"Warning: SQL for {table_name} references {ref}, not a fabricks-managed database")
        else:
            graph[table_name] = set()

    if cycle := _find_cycle(graph):
        print(f"Circular dependency: {' -> '.join(cycle)}")
        errors += 1

    if verbose:
        _print_stats(jobs_per_step, topics_per_step, sql_files_per_step)

    outcome = "OK" if errors == 0 else f"{errors} error(s)"
    print(f"checked {len(config_files)} config file(s), {len(tables)} job(s): {outcome}")
    return errors > 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("runtime", nargs="?", default=".", type=Path)
    parser.add_argument(
        "--passthrough-step",
        action="append",
        default=[],
        dest="passthrough_steps",
        help="step name whose jobs don't need their own .sql file (repeatable)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="print a per-step table: job count, topic count, .sql file count"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="path to the runtime's main conf YAML (skips auto-discovery of fabricksconfig.json/pyproject.toml)",
    )
    args = parser.parse_args()

    conf_yaml = (args.runtime / args.config) if args.config else _find_conf_yaml(args.runtime)
    if conf_yaml is None:
        print(
            "Warning: no fabricksconfig.json/pyproject.toml found -- bronze/silver steps won't be "
            "auto-exempted from the .sql file check; pass --config or --passthrough-step if needed"
        )

    passthrough_steps = (_passthrough_steps_from_conf(conf_yaml) if conf_yaml else frozenset()) | frozenset(
        args.passthrough_steps
    )
    if check_config(args.runtime, passthrough_steps, args.verbose):
        sys.exit(1)


if __name__ == "__main__":
    main()
