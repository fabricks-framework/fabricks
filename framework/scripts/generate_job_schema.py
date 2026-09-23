"""Regenerate job-schema.json (and one per layer) from the Pydantic job
config models, at the repo root.

tests/spark/databricks/runtime/**/_config.*.yml files reference these
via a `# yaml-language-server: $schema=...` comment. The per-layer files
(job-schema-bronze.json, job-schema-silver.json, job-schema-gold.json)
narrow to that layer's own options for better editor typing -- e.g. a
gold job's yaml points at job-schema-gold.json instead of the combined
schema, which allows options from every layer.

Usage:
    python scripts/generate_job_schema.py
"""

from pathlib import Path

from fabricks.models.job_schema import get_job_schema

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SCHEMAS = {
    "job-schema.json": None,
    "job-schema-bronze.json": "bronze",
    "job-schema-silver.json": "silver",
    "job-schema-gold.json": "gold",
}


def main() -> None:
    for filename, step in _SCHEMAS.items():
        path = _REPO_ROOT / filename
        path.write_text(get_job_schema(step) + "\n")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
