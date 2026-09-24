# Spark Query Plans

Notes for investigating Spark/Delta performance issues in this codebase by
reading real physical plans, not just SQL/template text. Used to diagnose
[#184](https://github.com/fabricks-framework/fabricks/issues/184),
[#202](https://github.com/fabricks-framework/fabricks/issues/202), and
[#203](https://github.com/fabricks-framework/fabricks/issues/203).

## Read the physical plan, not the SQL text

A CTE referenced once by name in generated SQL is not necessarily read once.
`.explain(mode="formatted")` prints each physical operator once, as a
numbered definition (`(14) Scan parquet ...`), then prints every place that
operator is reused as a reference back to that number (`+- Scan parquet ...
(14)`). Counting substring occurrences of a table/CTE name in the plan text
overcounts: it matches both the one real definition and every reference to
it. To count actual physical re-reads, match definition lines only:

```python
import re
re.findall(r"^\(\d+\) Scan parquet .*<table>", plan_text, re.MULTILINE)
```

## Spark does not always reuse a CTE across differently-pruned consumers

A `WITH` CTE referenced by name from multiple downstream branches is not
reliably computed once and shared, even though the SQL text only defines it
once. If each consumer prunes the CTE to a different column subset, Spark's
Catalyst optimizer can fail to recognize the pruned variants as the same
subplan and re-executes the CTE's defining query once per distinct shape
(confirmed via #202: one `__current` CTE, referenced from 2 places, produced
14 physical target-table scans once the rectify chain's own internal
self-joins were accounted for).

Whether this duplication actually matters depends on what is being
duplicated, not just how many times:

- **Expensive to repeat** (a Parquet/Delta scan from remote storage): fix it.
  See #202 -- materializing the relevant subset once as a cached global temp
  view, then pointing consumers at that view instead of the raw table, took
  physical scans of the target from 14 to 1.
- **Cheap to repeat** (an `ExistingRDD` already materialized in driver
  memory): not worth fixing. See #203 -- the same duplication pattern shows
  up on the batch/source side, but empirically (`.explain()` at multiple
  batch sizes, 10/1,000/20,000 rows) the reference count stays fixed and
  wall-clock cost does not scale with batch size, so caching it would add
  complexity for no measurable payoff.

Verify which case applies empirically (`.explain()` plus a real wall-clock
measurement at a couple of sizes) before assuming either way.

## Spark config does not fix this

Tried, for #202 (see that issue's comments for full numbers): excluding
Catalyst's `ColumnPruning` rule had no effect; excluding `InlineCTE` broke
Delta's own `MergeIntoCommand` outright (`PLAN_VALIDATION_FAILED_RULE_IN_BATCH`
/ `ReplaceCTERefWithRepartition`); disabling AQE only partially helped and
never reached a single scan, while being a broad session-wide change. There
is no clean config-level way to force CTE materialization in open-source
Spark -- use explicit `.cache()` / `CACHE TABLE` on the specific subset that
needs to be shared.

## Shuffle-heavy joins scale with data size, not batch size

Self-joins (e.g. `ctes/rectify.sql.jinja`'s `__rectified_next_operation`
fan-out) force `SortMergeJoin` -- shuffling and sorting both sides -- once
the joined data is non-trivial in size. Unlike a per-batch-source-filtered
scan (bounded by the incremental batch), a self-join over the merge's full
candidate row set scales with the **target's** size. Look for `Exchange` and
`SortMergeJoin` node counts in the plan, not just scan counts, when
investigating a merge query's cost on a large target (see #203, point 2,
not yet fixed).
