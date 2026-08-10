# Fixture topics

The bronze/silver `_config.*.yml` fixtures use royal-family topic names (`king`,
`queen`, `monarch`, `regent`, `royal`, `prince`, `princess`). The names don't hint
at what each one tests — this maps topic to actual data layout / scenario.

**Bronze read mode** — `king`, `queen`, `monarch`, `princess` use **streaming (legacy)**
(read landing files directly); `regent`, `royal`, `prince` use **register** mode
(a pre-built delta table as the input).

| Topic | Mode | Data | What it tests |
|---|---|---|---|
| `king` | streaming (legacy) | own json, append mode, deletelog | source A of a two-parent merge |
| `queen` | streaming (legacy) | own json, append mode, deletelog | source B of a two-parent merge — `silver.king_and_queen` merges `king` + `queen` (multi-parent CDC) |
| `monarch` | streaming (legacy) | reuses `king`+`queen` json under an alias (`_alias_paths` in `tests/integration/helpers/seed.py`), as if already a single merged source | extender chaining (`monarch`, `add_country`) |
| `regent` | register | same aliased `king`+`queen` json | register mode + a generic extender |
| `royal` | register | same aliased `king`+`queen` json | forced full-reload (`royal` extender, `__operation=reload`) |
| `prince` | register | own small json, no deletelog folder aliasing | special-character key columns (`@Id`) + deletelog handling |
| `princess` | streaming (legacy) | own json, parquet parser | catch-all matrix of bronze/silver job options: encrypt, schema drift, type widening, dedup ordering, calculated columns, checks, manual load, combine, missing/extra columns |

`monarch`/`regent`/`royal` don't have their own raw data — they're fed `king`+`queen`'s
files (see `_TOPIC_SOURCES` and `_alias_paths` in `tests/integration/helpers/seed.py`) so the
"already merged" scenarios stay in sync with the two-source data without duplicating it.

## Intentional config exceptions

These bronze `uri`s look inconsistent but are deliberate — they *are* the scenario under
test, not drift. Do not "standardize" them away:

- `royal/append` → `raw/delta/1/royal`: the `/1/` is a Unity-Catalog copy tier; `royal/latest`
  already uses `raw/delta/royal`, and the same delta table can't back two register jobs.
- `princess/no_column` → `raw/delta/no_column` (register) and `princess/too_many_columns` →
  `raw/too_many_columns`: purpose-built edge-case inputs.

The spread of `mode` values (`append`/`memory`/`register` in bronze;
`update`/`latest`/`append`/`combine`/`memory` in silver) is the permutation matrix the suite
exercises — also intentional.

A job with **no `tags`** is intentionally *not* scheduled — it exists only to check that its
config parses/validates, or to be pulled in directly (`get_job().run()`) or as a parent. Don't
add tags to "fix" them.
