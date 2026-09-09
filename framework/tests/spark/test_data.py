from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Literal

import pandas as pd

ITERATIONS = tuple(range(1, 12))
ENTITIES = ("king", "queen")
SPARK_TEST_ROOT = Path(__file__).parent
RAW_FIXTURES_ROOT = SPARK_TEST_ROOT / "fixtures"
APACHE_FIXTURES_ROOT = SPARK_TEST_ROOT / "apache" / "fixtures"
EXPECTED_ROOT = SPARK_TEST_ROOT / "expected"


def derive_source_rows(entity_dir: Path, source: str, operation: str | None = None) -> list[dict]:
    rows = []
    for source_file in sorted(entity_dir.rglob("*.jsonl")):
        timestamp = _timestamp_from_path(source_file)
        frame = pd.read_json(source_file, orient="records", lines=True)
        for record in frame.to_dict(orient="records"):
            record = {key: (None if pd.isna(value) else value) for key, value in record.items()}
            record["__operation"] = operation or ("reload" if record.get("BEL_IsFullLoad") else "upsert")
            record = {key: value for key, value in record.items() if not key.startswith("BEL_")}
            record.update(__timestamp=timestamp, __source=source)
            rows.append(record)
    return rows


def derive_entity_rows(iteration: int, entity: Literal["king", "queen"]) -> list[dict]:
    root = RAW_FIXTURES_ROOT / f"iter{iteration}"
    rows = derive_source_rows(root / entity, source=entity)
    delete_log = root / f"{entity}__deletelog"
    if delete_log.is_dir():
        rows += derive_source_rows(delete_log, source=entity, operation="delete")
    return rows


def read_ndjson(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def write_ndjson(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as output:
        for row in rows:
            output.write(json.dumps(row, default=str) + "\n")


def apache_fixture_paths(iteration: int) -> list[Path]:
    root = APACHE_FIXTURES_ROOT / f"iter{iteration}"
    return [path for entity in ENTITIES if (path := root / f"bronze_{entity}.jsonl").exists()]


def load_entity_frames(spark, iteration: int):
    return [spark.read.json(str(path)) for path in apache_fixture_paths(iteration)]


def load_combined_frame(spark, iteration: int):
    return spark.read.json(str(APACHE_FIXTURES_ROOT / f"iter{iteration}" / "king_queen.jsonl"))


def read_expected_rows(iteration: int) -> list[dict]:
    rows = read_ndjson(EXPECTED_ROOT / "scd2" / f"iter{iteration:02}.jsonl")
    for row in rows:
        row["__valid_from"] = _utc_timestamp(row["__valid_from"])
        row["__valid_to"] = _utc_timestamp(row["__valid_to"])
        if isinstance(row.get("newField"), str):
            row["newField"] = {"true": True, "false": False, "null": None}[row["newField"]]
    return rows


def validate_iteration(iteration: int) -> None:
    expected_combined = []
    for entity in ENTITIES:
        derived = derive_entity_rows(iteration, entity)
        committed = APACHE_FIXTURES_ROOT / f"iter{iteration}" / f"bronze_{entity}.jsonl"
        if derived:
            if not committed.exists() or read_ndjson(committed) != derived:
                raise ValueError(f"iteration {iteration} {entity} fixture is stale")
            expected_combined += derived
        elif committed.exists():
            raise ValueError(f"iteration {iteration} {entity} fixture should be absent")

    combined = APACHE_FIXTURES_ROOT / f"iter{iteration}" / "king_queen.jsonl"
    if read_ndjson(combined) != expected_combined:
        raise ValueError(f"iteration {iteration} combined fixture is stale")
    if not read_expected_rows(iteration):
        raise ValueError(f"iteration {iteration} expected state is empty")


def _timestamp_from_path(source_file: Path) -> str:
    year, month, day, batch = source_file.parent.parts[-4:]
    hhmmss = (batch + "00")[:6]
    return f"{year}-{month}-{day}T{hhmmss[0:2]}:{hhmmss[2:4]}:{hhmmss[4:6]}"


def _utc_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
