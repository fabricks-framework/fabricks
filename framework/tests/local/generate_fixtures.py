"""Derive CDC-ready NDJSON rows from the raw JSON landing fixtures under
tests/data/. Run once, output committed to tests/local/fixtures/ —
see docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #6.

Ported from tests/databricks/utils.py's convert_json_to_parquet/
convert_parquet_to_delta (which import databricks.sdk.runtime and can't run
outside a Databricks notebook): same pandas.read_json read, same folder-path
__timestamp derivation, no Spark/parquet write, no Unity-Catalog replication.
"""

import argparse
import json
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DATA_ROOT = _REPO_ROOT / "framework" / "tests" / "data"
_FIXTURES_ROOT = _REPO_ROOT / "framework" / "tests" / "local" / "fixtures"


def _timestamp_from_path(json_file: Path) -> str:
    """Mirror convert_parquet_to_delta's folder-path timestamp derivation:
    the parent dir is .../YYYY/MM/DD/NNNN, giving 'YYYY-MM-DDT00:NN:00' —
    minute-of-day encoded from the batch number, hour fixed at 00, matching
    the original's `slice(__split, __split_size - 4, 4)` over year/month/day/batch.
    """
    parts = json_file.parent.parts
    year, month, day, batch = parts[-4], parts[-3], parts[-2], parts[-1]
    minute = int(batch) % 60
    return f"{year}-{month}-{day}T00:{minute:02d}:00"


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
    tests/data/job1-9, so it's a no-op for this fixture set.
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


# job1 through job9: the verified full range where both king and queen have
# plain (non-deletelog) raw data (job10 has queen__deletelog only, job11 has
# no queen folder at all — see "Out of scope"). Adding job10/11 needs new
# design work (delete-log or entity-optional merge handling), not just a
# list entry — see docs/adr/0001-...md's job-sequential strategy.
_JOB_NUMBERS = [1, 2, 3, 4, 5, 6, 7, 8, 9]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    for job_num in _JOB_NUMBERS:
        job_dir = f"job{job_num}"
        for entity in ("king", "queen"):
            entity_dir = _DATA_ROOT / job_dir / entity
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
            write_ndjson(rows, _FIXTURES_ROOT / job_dir / f"bronze_{entity}_scd1.jsonl")


if __name__ == "__main__":
    main()
