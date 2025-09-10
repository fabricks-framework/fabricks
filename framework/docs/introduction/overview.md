# Key Concepts Overview

This page introduces Fabricks core concepts so you can read and write pipelines with confidence.

**What Fabricks is ?**

- SQL-first transformations
- YAML-based orchestration for jobs, steps, schedules, and dependencies
- Built-in Change Data Capture (CDC) patterns (SCD1/SCD2)
- Data Quality gates via checks and contracts
- Extensible with UDFs, Parsers, and Python Extenders

**Jobs vs Steps**

- *Step*: A processing layer/type that defines behavior and defaults (Bronze, Silver, Gold).
- *Job*: A concrete unit of work within a step (identified by topic + item) that you schedule/run.

**Layers (Bronze → Silver → Gold)**

- Bronze: Land raw data (append/register/memory). Keep logic light.
- Silver: Standardize/clean/enrich. Optional CDC (SCD1/SCD2) and convenience views.
- Gold: Consumption models and marts. Supports merge/update and SCD patterns.

**Change Data Capture (CDC)**

- SCD1: Current state with optional soft deletes (__is_current, __is_deleted) and a {table}__current view.
- SCD2: Historical validity windows (__valid_from, __valid_to). Gold consumers use (__key, __timestamp, __operation).
- Reload rectification: A snapshot boundary that reconciles target state (see CDC reference).

**Data Quality (Checks)**

- Pre- and post-run checks, contracts, and row count thresholds.
- CI-friendly: Fail fast on contract violations or warn when needed.

**Extensibility**

- UDFs: Register functions on the Spark session for use in SQL.
- Parsers: Custom readers/cleaners for sources (return a DataFrame).
- Extenders: Small Python transforms applied right before writing.

Navigation

- Layers guide: Layers (Bronze/Silver/Gold) → [Layers](./layers.md)
- CDC strategies and contracts → [CDC](../reference/cdc.md)
- Step references → [Bronze](../steps/bronze.md) • [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Data Quality → [Checks & Data Quality](../reference/checks-data-quality.md)
- Table and storage options → [Table Options](../reference/table-options.md)
- Extensibility → [Extenders, UDFs & Parsers](../reference/extenders-udfs-parsers.md)
