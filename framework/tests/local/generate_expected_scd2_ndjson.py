"""Chain job02.sql..job11.sql (hand-authored SCD2 fixture views) forward from
job01.ndjson into job02.ndjson..job11.ndjson.

Each job{N}.sql (N=2..11) under `tests/expected/silver/scd2/` is, mechanically:

    select <9-col cast header>
    from values (<job N's own new/changed rows, hand-authored>)
    union all
    select <same cols> from expected.silver_scd2_job{N-1} where not __is_current
    order by id, __valid_from

That `union all` is a static computation over already-known data (no
CDC-under-test merge logic involved: just "this job's own new rows, plus
whichever earlier rows are no longer current"), so it's reproduced here in
pure Python at generation time instead of via a live Spark view: parse
job{N}.sql's own VALUES rows, union with job{N-1}'s non-current rows (read
from the already-produced job{N-1}.ndjson), sort, write NDJSON.

Does NOT delete or modify the .sql files, and does NOT touch
create_expected_views()/_create_views() (tests/databricks/utils.py) -- both
are explicitly out of scope, left for a follow-up task.

See .superpowers/sdd/2026-09-01-local-cdc-ddl-tests-stage1/task-3b-brief.md.
"""

import json
import re
from pathlib import Path

EXPECTED_DIR = Path(__file__).resolve().parent.parent / "expected" / "silver" / "scd2"

_CAST_RE = re.compile(r"cast\(col(\d+)\s+as\s+(\w+)\)\s+as\s+`?(\w+)`?", re.IGNORECASE)
_STRING_LITERAL_RE = re.compile(r"'((?:[^'\\]|\\.)*)'")


def parse_header(sql: str) -> list[tuple[str, str]]:
    """Return [(column_name, sql_type), ...] in col1..colN order, parsed from
    the file's own `cast(colN as type) as name` lines (schema isn't
    hardcoded per job -- read from each file)."""
    matches = sorted(
        ((int(colnum), sql_type, name) for colnum, sql_type, name in _CAST_RE.findall(sql)),
        key=lambda m: m[0],
    )
    return [(name, sql_type) for _, sql_type, name in matches]


def cast_value(raw: str, sql_type: str):
    sql_type = sql_type.lower()
    if sql_type in ("timestamp", "string"):
        return raw
    if sql_type == "int":
        return int(raw)
    if sql_type == "double":
        return float(raw)
    if sql_type == "boolean":
        return raw.strip().lower() == "true"
    raise ValueError(f"unsupported sql cast type: {sql_type!r}")


def parse_values_rows(sql: str, header: list[tuple[str, str]]) -> list[dict]:
    """Parse the file's own `from values (...), (...), ...` block -- i.e.
    everything between the `values` keyword and `union all` -- into row dicts
    keyed/typed per `header`."""
    values_start = re.search(r"\bvalues\b", sql, re.IGNORECASE).end()
    union_start = re.search(r"\bunion\s+all\b", sql, re.IGNORECASE).start()
    block = sql[values_start:union_start]
    rows = []
    for tuple_match in re.finditer(r"\(([^()]*)\)", block):
        raw_values = _STRING_LITERAL_RE.findall(tuple_match.group(1))
        if len(raw_values) != len(header):
            raise ValueError(f"expected {len(header)} values, got {len(raw_values)}: {raw_values}")
        rows.append({name: cast_value(v, sql_type) for (name, sql_type), v in zip(header, raw_values)})
    return rows


def carry_forward(prev_rows: list[dict], header_names: list[str]) -> list[dict]:
    """job{N-1}'s non-current rows, reshaped to job{N}'s column set.

    `row.get(name)` naturally yields None for any column job{N-1} doesn't
    have yet -- this is what implements the job1->job2 `newField`
    schema-boundary handling (job2's SQL does `null as newField` explicitly
    for this exact case), with no special-casing needed for any other step.
    """
    return [
        {name: row.get(name) for name in header_names}
        for row in prev_rows
        if not row["__is_current"]
    ]


def build_job_rows(job_num: int, prev_rows: list[dict]) -> list[dict]:
    sql = (EXPECTED_DIR / f"job{job_num:02d}.sql").read_text(encoding="utf-8")
    header = parse_header(sql)
    header_names = [name for name, _ in header]
    own_rows = parse_values_rows(sql, header)
    forwarded_rows = carry_forward(prev_rows, header_names)
    combined = own_rows + forwarded_rows
    combined.sort(key=lambda r: (r["id"], r["__valid_from"]))  # order by id, __valid_from
    return combined


def read_ndjson(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_ndjson(rows: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")


def main() -> None:
    prev_rows = read_ndjson(EXPECTED_DIR / "job01.ndjson")
    for job_num in range(2, 12):
        rows = build_job_rows(job_num, prev_rows)
        write_ndjson(rows, EXPECTED_DIR / f"job{job_num:02d}.ndjson")
        prev_rows = rows


if __name__ == "__main__":
    main()
