"""Regenerate committed Apache CDC fixtures from the canonical raw data."""

import argparse

from tests.spark.test_data import APACHE_FIXTURES_ROOT, ENTITIES, ITERATIONS, derive_entity_rows, write_ndjson


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()

    for iteration in ITERATIONS:
        combined = []
        for entity in ENTITIES:
            rows = derive_entity_rows(iteration, entity)
            if not rows:
                continue
            write_ndjson(
                rows,
                APACHE_FIXTURES_ROOT / f"iter{iteration}" / f"bronze_{entity}.jsonl",
            )
            combined += rows
        write_ndjson(combined, APACHE_FIXTURES_ROOT / f"iter{iteration}" / "king_queen.jsonl")


if __name__ == "__main__":
    main()
