# Silver Step Reference

The Silver step standardizes and enriches data, and applies CDC (SCD1/SCD2) if configured.

## Modes

| Mode    | Description                                                                              |
|---------|------------------------------------------------------------------------------------------|
| memory  | View-only; no table written.                                                             |
| append  | Append-only table.                                                                       |
| latest  | Keep only the latest row per key within the batch.                                       |
| update  | Merge/upsert semantics; typically used with CDC.                                         |
| combine | Combine/union outputs from parent jobs into one result.                                  |

### Behavior notes

- Deduplication and ordering
  - `deduplicate` drops duplicate keys within the current batch.
  - If `order_duplicate_by` is provided, rows are ordered by the specified sort before deduplication; the first row per key is kept.
  - In `latest` mode, only within-batch keys are considered (historical rows are not consulted). In `update` mode, winners are merged against the existing table.

- Streaming constraints
  - `stream: true` is supported where your connectors/environment support streaming.
  - In `append` mode it appends micro-batches; in `latest`/`update` modes semantics rely on deterministic keys and, if used, stable ordering columns.
  - With streaming, `pre_run` counts can be zero; prefer `post_run` checks for row count bounds.

- Combine behavior
  - `combine` mode unions all parent outputs into a single logical result.
  - Parents should be schema-compatible; columns are aligned by name. No de-duplication is performed unless `deduplicate` is enabled.

## CDC strategies

| Strategy | What it does                                                                                         | Convenience views |
|----------|-------------------------------------------------------------------------------------------------------|-------------------|
| nocdc    | No CDC flags; writes the result as-is.                                                                | —                 |
| scd1     | Tracks current vs deleted; adds flags `__is_current`, `__is_deleted`.                                 | `TABLE__current`  |
| scd2     | Maintains validity windows with `__valid_from`, `__valid_to` and historical rows.                     | `TABLE__current`  |

## Options at a glance

| Option              | Purpose                                                                                     |
|---------------------|---------------------------------------------------------------------------------------------|
| type                | `default` vs `manual` (manual: you manage persistence yourself).                            |
| mode                | One of the modes listed above.                                                              |
| change_data_capture | CDC strategy: `nocdc` \| `scd1` \| `scd2`.                                                  |
| parents             | Upstream job identifiers (e.g., `bronze.demo_source`).                                      |
| filter_where        | SQL predicate applied at transform time.                                                    |
| deduplicate         | Drop duplicate keys within the batch.                                                       |
| stream              | Enable streaming semantics where supported.                                                 |
| order_duplicate_by  | Choose preferred row when duplicates exist (e.g., `{ order_by: desc }`).                    |
| timeout             | Per-job timeout seconds (overrides step default).                                           |
| extender            | Name of a Python extender to apply (see Extenders).                                         |
| extender_options    | Arguments for the extender (mapping).                                                       |
| check_options       | Configure DQ checks (pre_run, post_run, max_rows, min_rows, count_must_equal, skip).        |

#### Option matrix (types • defaults • required)

| Option              | Type                                  | Default | Required | Description                                                                                 |
|---------------------|---------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| type                | enum: default, manual                 | default | no       | `manual` disables auto DDL/DML; you manage persistence yourself.                            |
| mode                | enum: memory, append, latest, update, combine | —       | yes      | Processing behavior.                                                                        |
| change_data_capture | enum: nocdc, scd1, scd2               | nocdc   | no       | CDC strategy applied when writing.                                                          |
| parents             | array[string]                         | —       | no       | Upstream job identifiers to enforce ordering.                                               |
| filter_where        | string (SQL predicate)                 | —       | no       | Predicate applied at transform time.                                                        |
| deduplicate         | boolean                                | false   | no       | Drop duplicate keys within the current batch.                                               |
| order_duplicate_by  | map (e.g., { columns: [ts], order_by: desc }) | — | no       | Choose preferred row for duplicates before `deduplicate`.                                   |
| stream              | boolean                                | false   | no       | Enable streaming semantics where supported.                                                 |
| timeout             | integer (seconds)                      | —       | no       | Per-job timeout; overrides step default.                                                    |
| extender            | string                                 | —       | no       | Python extender to transform the DataFrame.                                                 |
| extender_options    | map[string,any]                        | {}      | no       | Arguments passed to the extender.                                                           |
| check_options       | map                                     | —       | no       | Data quality checks (see Checks & Data Quality).                                            |

Notes:
- If both `order_duplicate_by` and `deduplicate` are set, ordering is applied first, then the first row per key is retained.
- `latest` considers only the current batch; use `update` if you need to merge winners against an existing table.

## Field reference

- *Core*
    - **type**: `default` vs `manual`. Manual means Fabricks won’t auto-generate DDL/DML; you control persistence.
    - **mode**: Processing behavior (`memory`, `append`, `latest`, `update`, `combine`).
    - **timeout**: Per-job timeout; overrides step defaults.

- *CDC*
    - **change_data_capture**: `nocdc`, `scd1`, or `scd2`.
    - **scd1 flags**: `__is_current`, `__is_deleted` and a `{table}__current` convenience view.
    - **scd2 windows**: `__valid_from`, `__valid_to` and a `{table}__current` convenience view.

- *Dependencies & ordering*
    - **parents**: Explicit upstream jobs (e.g., `bronze.topic_item`).
    - **deduplicate**: Drop duplicate keys within the batch.
    - **order_duplicate_by**: Pick a preferred row when duplicates exist (e.g., `{ order_by: desc }`).

- *Streaming*
    - **stream**: Where supported, use streaming semantics for incremental processing.

- *Extensibility*
    - **extender**: Apply a Python extender to the DataFrame.
    - **extender_options**: Mapping of arguments passed to the extender.
  - See also: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)

- Checks
  - check_options: Configure DQ checks (e.g., `pre_run`, `post_run`, `max_rows`, `min_rows`, `count_must_equal`, `skip`). See [Checks & Data Quality](../reference/checks-data-quality.md)

Minimal example
```yaml
- job:
    step: silver
    topic: demo
    item: scd1
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.demo_source]
```

---

### Silver options

See Options at a glance and Field reference above.
### Silver jobs

- Combine outputs example:
  ```yaml
  - job:
      step: silver
      topic: princess
      item: combine
      options:
        mode: combine
        parents:
          - silver.princess_extend
          - silver.princess_latest
  ```

- Monarchs with SCD1/SCD2 and delta/memory flavors:

```yaml
- job:
    step: silver
    topic: monarch
    item: scd1
    options:
      mode: update
      change_data_capture: scd1
- job:
    step: silver
    topic: monarch
    item: scd2
    options:
      mode: update
      change_data_capture: scd2
- job:
    step: silver
    topic: monarch
    item: delta
    options:
      mode: update
      change_data_capture: scd2
- job:
    step: silver
    topic: monarch
    item: memory
    options:
      mode: memory
      change_data_capture: nocdc
```

- Multi-parent example (kings and queens):

```yaml
- job:
    step: silver
    topic: king_and_queen
    item: scd1
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.queen_scd1, bronze.king_scd1]
```

- Princess patterns: latest, append, calculated columns, duplicate ordering, manual type, schema drift, extend with extenders:

```yaml
- job:
    step: silver
    topic: princess
    item: latest
    options:
      mode: latest
- job:
    step: silver
    topic: princess
    item: append
    options:
      mode: append
- job:
    step: silver
    topic: princess
    item: order_duplicate
    options:
      mode: update
      change_data_capture: scd1
      order_duplicate_by:
        order_by: desc
- job:
    step: silver
    topic: princess
    item: calculated_column
    options:
      mode: update
      change_data_capture: scd1
      order_duplicate_by:
        order_by: asc
- job:
    step: silver
    topic: princess
    item: manual
    options:
      type: manual
      mode: update
      change_data_capture: scd1
- job:
    step: silver
    topic: princess
    item: extend
    options:
      mode: update
      change_data_capture: scd1
    extender_options:
        - extender: add_country
        arguments:
          country: Belgium
```

- Quality gate with `check_options`:
  ```yaml
  - job:
      step: silver
      topic: princess
      item: quality_gate
      options:
        mode: update
        change_data_capture: scd1
        parents: [silver.princess_latest]
      check_options:
        post_run: true
        min_rows: 1
        max_rows: 100000
  ```

## Related

- Next steps: [Gold Step](./gold.md), [Table Options](../reference/table-options.md)
- Data quality: [Checks & Data Quality](../reference/checks-data-quality.md)
- Extensibility: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)
- Sample runtime: [Sample runtime](../helpers/runtime.md#sample-runtime)

Special characters in column names are preserved (see `prince.special_char`).
