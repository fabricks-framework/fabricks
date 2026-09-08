"""Derive CDC-ready NDJSON rows from the raw JSON landing fixtures under
tests/spark/fixtures/ -- only king/queen (+ their __deletelog siblings) are
read; every other topic that directory used to hold (prince, princess,
monarch, regent, ...) belonged to the old topic-zoo Databricks suite (see
_archive/databricks-old/) and has been moved to _archive/fixtures-old-topics/
since nothing here reads it. Run once, output committed to
tests/spark/apache/fixtures/ (this tier's own derived NDJSON cache) — see
docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #6.

Originally ported from the archived tests/spark/databricks/utils.py's
convert_json_to_parquet/convert_parquet_to_delta (which imported
databricks.sdk.runtime and couldn't run outside a Databricks notebook): same
pandas.read_json read, same folder-path __timestamp derivation, no
Spark/parquet write, no Unity-Catalog replication.
"""

import argparse
import json
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[4]
_DATA_ROOT = _REPO_ROOT / "framework" / "tests" / "spark" / "fixtures"
_FIXTURES_ROOT = _REPO_ROOT / "framework" / "tests" / "spark" / "apache" / "fixtures"


def _timestamp_from_path(json_file: Path) -> str:
    """Mirror convert_parquet_to_delta's folder-path timestamp derivation
    exactly: `left(concat_ws('', slice(__split, __split_size - 4, 4), '00'), 14)`
    then `to_timestamp(..., 'yyyyMMddHHmmss')` — i.e. concatenate
    year+month+day+batch+"00", take the first 14 characters, and parse that
    as yyyyMMddHHmmss. Since year+month+day always contribute exactly 8
    characters, this reduces to: HHMMSS = (batch + "00")[:6], hour fixed at
    00 for every batch folder actually present under tests/spark/fixtures (all start
    with "00").

    NOT simply "minute = int(batch) % 60, second = 0" (the previous version
    of this function) -- that only coincidentally matches for a 4-digit
    batch like "0001" (int(batch) % 60 == 1 == the string slice's minute
    digits "01"). It silently diverges for any batch string longer than 4
    digits: verified against tests/spark/fixtures/iter3/king/2022/03/01/001234/ (a real
    6-digit batch folder) and tests/spark/expected/scd2/iter03.jsonl's chained
    VALUES rows -- the real formula gives "00:12:34" (batch "001234"[:6],
    since batch alone is already 6 chars: HH="00" MM="12" SS="34"), while
    int("001234") % 60 == 34 gives the wrong "00:34:00".
    """
    parts = json_file.parent.parts
    year, month, day, batch = parts[-4], parts[-3], parts[-2], parts[-1]
    hhmmss = (batch + "00")[:6]
    hour, minute, second = hhmmss[0:2], hhmmss[2:4], hhmmss[4:6]
    return f"{year}-{month}-{day}T{hour}:{minute}:{second}"


def derive_rows(entity_dir: Path, source: str, operation: str | None = None) -> list[dict]:
    """Mirror monarch.py's DeleteLogBaseParser/MonarchParser row derivation.

    - `__operation`: 'reload' if BEL_IsFullLoad is truthy else 'upsert'
      (DeleteLogBaseParser._parse), unless `operation` is given, in which
      case every row gets that fixed value — used for __deletelog rows,
      which _parse_delete_log always tags 'delete' regardless of their own
      BEL_IsFullLoad.
    - All BEL_* columns are dropped (DeleteLogBaseParser.parse drops
      BEL_IsFullLoad/BEL_UpdateDateUtc/BEL_DeleteDateUtc, MonarchParser.parse
      drops any others, e.g. BEL_RestoredDateUtc — dropping the whole prefix
      here covers both in one pass).

    Not replicated: DeleteLogBaseParser.nullify()'s sentinel-date-to-null
    logic (string "1753-01-01 00:00:00.000" -> None on every non-__ column).
    Verified via grep that no "1753-01-01" value exists anywhere under
    tests/spark/fixtures/iter1-9, so it's a no-op for this fixture set.
    """
    rows: list[dict] = []
    # Raw fixtures are .jsonl (NDJSON), not .json — converted by Task 2, moved by Task 3.
    # Glob pattern must match .jsonl or it silently produces zero rows.
    for json_file in sorted(entity_dir.rglob("*.jsonl")):
        timestamp = _timestamp_from_path(json_file)
        # Don't use convert_dates: null date fields would become NaT (pandas Not-a-Time),
        # which json.dumps(default=str) would serialize as literal string "NaT" in output.
        # Instead, NaN→None conversion below ensures proper JSON null. String dates pass
        # through unchanged (identical result to convert_dates + json serialization, but
        # avoids the NaT corruption of null fields).
        df = pd.read_json(json_file, orient="records", lines=True)
        for record in df.to_dict(orient="records"):
            # Replace NaN with None so null fields serialize as JSON null, not string "NaT"
            record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
            record["__operation"] = operation or ("reload" if record.get("BEL_IsFullLoad") else "upsert")
            record = {k: v for k, v in record.items() if not k.startswith("BEL_")}
            record["__timestamp"] = timestamp
            record["__source"] = source
            rows.append(record)
    return rows


def write_ndjson(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")


# iter1-9 have plain (non-deletelog) raw data for both king and queen.
# iter10 has queen__deletelog only (a single delete op, nothing to delete
# against — a legitimate no-op batch); iter11 has no queen data at all,
# plain or deletelog. Both are still included: main() below skips writing
# a fixture file when an entity has zero rows, and king_and_queen_built
# (tests/spark/apache/conftest.py) skips a missing file when building that
# iteration's incoming batch — the target table's existing queen state
# (from an earlier iteration) is then left untouched by that iteration's
# merge, same as a real incremental run where an entity simply has no new
# data that batch.
_ITER_NUMBERS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    for iter_num in _ITER_NUMBERS:
        iter_dir = f"iter{iter_num}"
        king_queen_rows = []
        for entity in ("king", "queen"):
            entity_dir = _DATA_ROOT / iter_dir / entity
            rows = derive_rows(entity_dir, source=entity)
            # Sibling __deletelog dir, if present, holds real delete events
            # (monarch.py's DeleteLogBaseParser._parse_delete_log). Appended
            # after the main rows, matching concat_dfs([df, df_del])'s order
            # in the real parser — the merge's own __timestamp-ordered window
            # functions do the actual historization, so append order between
            # the two sources doesn't affect the result.
            deletelog_dir = entity_dir.parent / f"{entity}__deletelog"
            if deletelog_dir.is_dir():
                rows += derive_rows(deletelog_dir, source=entity, operation="delete")
            if not rows:
                # No plain data and no deletelog for this entity this
                # iteration (iter11's queen) -- write nothing rather than
                # an empty .jsonl, which spark.read.json() can't infer a
                # schema from. Absence of the file is itself the signal
                # king_and_queen_built uses to skip this entity.
                continue
            write_ndjson(rows, _FIXTURES_ROOT / iter_dir / f"bronze_{entity}.jsonl")
            king_queen_rows += rows

        # king_queen.jsonl: the same rows as bronze_king.jsonl + bronze_queen.jsonl,
        # concatenated into one file -- mimics a single combined bronze topic
        # (like the real "monarch" topic, which lands king- and queen-shaped
        # rows through one raw source rather than two separate ones; see
        # _archive/fixtures-old-topics/iter4-monarch for a real example of that shape).
        # Each row keeps its own __source ("king"/"queen"), same as
        # bronze_{entity}.jsonl -- this only changes which *file* the rows
        # arrive in, not the row content.
        write_ndjson(king_queen_rows, _FIXTURES_ROOT / iter_dir / "king_queen.jsonl")


if __name__ == "__main__":
    main()
