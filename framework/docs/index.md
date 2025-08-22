# Fabricks User Guide

Fabricks is a pragmatic framework to build Databricks Lakehouse pipelines using YAML for orchestration and SQL for transformations. It standardizes jobs, steps, schedules, CDC, and checks while keeping development SQL‑first.

This landing page provides concise summaries and links to detailed references.

---

## Step Summaries

High‑level overview of each pipeline step. See each step page for modes, options, and examples.

### Bronze
Raw ingestion from source systems (files, streams, existing tables). Keep logic light; land data for downstream processing.
- Typical modes: memory, append, register
- Output: raw tables or temporary views
- Read the full reference → [Bronze](steps/bronze.md)

### Silver
Standardize, clean, and enrich data; optionally apply CDC (SCD1/SCD2). Produces conformed datasets and convenience views.
- Typical modes: memory, append, latest, update, combine
- CDC: nocdc, scd1, scd2 with built‑in helpers
- Read the full reference → [Silver](steps/silver.md)

### Gold
Curated business models for analytics and reporting; dimensional or mart‑style outputs. Can also invoke notebooks when needed.
- Typical modes: memory, append, complete, update, invoke
- Read the full reference → [Gold](steps/gold.md)

### Semantic
Apply table properties/metadata and optional copy operations to standardize governance and table features.
- Mode: complete (apply properties/metadata/copy)
- Read the full reference → [Semantic](steps/semantic.md)

---

## Where to Configure

- Project configuration, schedules, and structure → [Runtime](runtime.md)
- Data quality and rollback behavior → [Checks & Data Quality](reference/checks-data-quality.md)
- Table properties, clustering, and layout → [Table Options](reference/table-options.md)
- Custom logic and reusable SQL assets → [Extenders, UDFs & Views](reference/extenders-udfs-views.md)

---

## Get Started

- Copy the minimal sample and adapt it: [Sample runtime](runtime.md#sample-runtime)
- Point `tool.fabricks.runtime` and `config` to your runtime and start adding jobs in SQL and YAML.
