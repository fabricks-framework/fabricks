# 0003: How the CDC update query was sped up, and what was not worth doing

## Context

A `mode="update"` SCD1/SCD2 merge query spent its time in shuffles, not scans:
the target read is already cached once (#202) and the rectify self-join is
already a broadcast (#203). Measured with `tests/spark/apache/test_cdc_update_benchmark.py`
(`BENCH_ROWS=400000`, local Spark): SCD1 5.2s, SCD2 2.55s. About 0.85s of each
run is Python-to-Spark round trips in `get_query` (row check, slice probe, current-view
cache), the rest is the generated query.

## Decision

Fix it in the SQL templates only, keeping one generated statement so the debug
output stays copy-pasteable and `explain`-able as a single plan.

- **scd1 `__scd1_last_key`**: one `row_number()` ordered "last upsert first,
  else first delete" replaces a `union all` of two windows plus a `not exists`
  self-join (61 -> 15 exchanges). Relies on a `delete` row never having a null
  `__key`; null keys only come from truncate/reload rows, which the filter drops.
- **rectify `__rectified_base`**: `__current` is unioned in only when the batch
  has a `reload` (or `has_no_data`). Without a reload every rectify branch
  resolves to a plain upsert and current rows are filtered out at the end, so the
  whole target slice was being shuffled for nothing. Incremental runs benefit;
  reload batches behave exactly as before.
- **scd1 `__merge_condition`**: `__operation` already is the condition, so the
  constant-table join is gone.
- **scd2**: `__scd2_rn` lives in `__scd2_base` so the `lead` and `row_number`
  windows share one Window operator. Plan-only win, no measurable timing change.

Tried and rejected, no measurable gain (do not retry without new evidence):

- A shared narrow CTE for the rectify timestamp and reload-candidate consumers:
  plan operator counts were identical, the two branches diverge into an aggregate
  and a filter straight after the union, so no shuffle can be reused.
- Skipping the current-view cache when the batch has no reload: SCD2 -15%, SCD1
  none, but it needs a Python reload probe that costs about what it saves and
  brings back the #202 multi-scan risk for reload batches.
- A cached Jinja `Environment` (~45ms) and `Table.has_rows` instead of
  `count(*)` in `get_query_context`: correct, but inside benchmark noise locally.

## Consequences

- Judge a template change by the benchmark median plus plan operator counts
  (definition lines only, see `docs/SPARK.md`), not by plan counts alone: the
  scd2 window merge changed the plan and not the time.
- Run the benchmark serially (`BENCH_ROWS=400000 just test-apache
  tests/spark/apache/test_cdc_update_benchmark.py`); pytest-benchmark disables
  itself under xdist.
- Not verified under Photon or on a cluster. Photon should keep the gain (it
  changes how operators run, not which ones the plan has), but check the Databricks
  tier before relying on it.
