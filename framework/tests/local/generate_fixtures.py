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


def derive_rows(entity_dir: Path, source: str) -> list[dict]:
    rows: list[dict] = []
    for json_file in sorted(entity_dir.rglob("*.jsonl")):
        timestamp = _timestamp_from_path(json_file)
        df = pd.read_json(json_file, orient="records", lines=True)
        for record in df.to_dict(orient="records"):
            # Replace NaN with None so it round-trips through JSON as null
            record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
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
            rows = derive_rows(_DATA_ROOT / job_dir / entity, source=entity)
            write_ndjson(rows, _FIXTURES_ROOT / job_dir / f"bronze_{entity}_scd1.jsonl")


if __name__ == "__main__":
    main()
