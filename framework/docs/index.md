# Fabricks User Guide

Fabricks is a pragmatic framework to build Databricks Lakehouse pipelines using YAML for orchestration and SQL for transformations. 
It standardizes jobs, steps, schedules, CDC, and checks while keeping development SQL‑first.

---

# Steps Overview

Fabricks organizes your Lakehouse into clear layers. Each step has a dedicated reference with modes, options, and examples.

### Bronze
Raw ingestion from source systems (files, streams, existing tables). Keep logic light; land data for downstream processing.

- Typical modes: memory, append, register
- Focus: lightweight parsing/landing; no business logic
- Output: raw tables or temporary views


Read the full reference → [Bronze Step](steps/bronze.md)

### Silver
Standardize, clean, and enrich data; optionally apply CDC (SCD1/SCD2). Produces conformed datasets and convenience views.

- Typical modes: memory, append, latest, update, combine
- CDC: nocdc, scd1, scd2 with built-in helpers and views
- Output: conformed tables and curated views

Read the full reference → [Silver Step](steps/silver.md)

### Gold
Curated business models for analytics and reporting; dimensional or mart‑style outputs. Can also invoke notebooks when needed.

- Typical modes: memory, append, complete, update, invoke (notebooks)
- Focus: dimensional models, marts, KPI-ready data
- Output: business-consumption tables and views

Read the full reference → [Gold Step](steps/gold.md)

---

## Where to Configure

- Project configuration, schedules, and structure → [Runtime](helpers/runtime.md)
- Data quality and rollback behavior → [Checks & Data Quality](reference/checks-data-quality.md)
- Table properties, clustering, and layout → [Table Options](reference/table-options.md)
- Custom logic and reusable SQL assets → [Extenders, UDFs & Parsers](reference/extenders-udfs-parsers.md)
- Change Data Capture (CDC) → [CDC](reference/cdc.md)

