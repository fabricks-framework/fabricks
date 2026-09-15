# Domain glossary

Names for concepts that recur across the codebase but don't otherwise have
a canonical home. Add a term here when a design review or discussion
sharpens or names one; don't pre-populate speculatively.

## Expected-state oracle

The checked-in `tests/spark/expected/{scd0,scd1,scd2}/iter*.{sql,jsonl}`
files, plus the logic in `tests/spark/expected/compare.py` that turns them
into queryable Spark views/tables (`create_expected_views`) and diffs a
job's real table state against them (`compare_to_expected`/
`assert_dfs_equal`). It is the sole source of truth for whether a CDC
merge (SCD0/SCD1/SCD2) produced the right rows — every `tests/spark/apache/
test_cdc.py`/`test_gold_cdc.py` scenario asserts against it rather than
against hand-written per-test expectations. See
[docs/adr/0001-duckdb-backend-for-local-cdc-tests.md](docs/adr/0001-duckdb-backend-for-local-cdc-tests.md),
Stage 1 item #7.
