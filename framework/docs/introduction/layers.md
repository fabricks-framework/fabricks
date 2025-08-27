# Layers (Bronze → Silver → Gold)

Fabricks organizes pipelines into layered steps to keep responsibilities clear and composable. Use this page to decide where logic belongs and how to structure data flow.

Layer summary

- Bronze: Land raw data “as-is” from files/streams/tables. Keep it light.
- Silver: Standardize, clean, enrich; optionally apply CDC (SCD1/SCD2). Provide conformed datasets and convenience views.
- Gold: Build consumption-ready models (facts/dims/marts), possibly with CDC merges or full refreshes.

When to use each layer

- Bronze
    - Ingest from external sources with minimal logic
    - Keep provenance and raw fidelity
    - Typical modes: memory, append, register

- Silver
    - Standardize schemas, apply business keys, deduplicate
    - Apply CDC: scd1/scd2 with convenience views (e.g., table__current)
    - Typical modes: memory, append, latest, update, combine

- Gold
    - Aggregate, reshape, and model data for analytics/reporting
    - Implement SCD patterns at the desired consumption grain
    - Typical modes: memory, append, complete, update, invoke (notebook)

Data flow

- Bronze: raw sources → (light parsing, optional filters/calculated columns) → raw tables/views
- Silver: bronze tables/views → (standardize/enrich, optional CDC) → conformed tables + {table}__current view
- Gold: silver conformed outputs → (marts/dim/fact, SCD merges or complete refresh) → business-consumption tables/views

Quality and governance

- Configure pre_run/post_run checks and row thresholds in step/job options.
- Use warning/fail contracts to protect downstream consumers.

Related

- Bronze: ../steps/bronze.md
- Silver: ../steps/silver.md
- Gold: ../steps/gold.md
- CDC reference: ../reference/cdc.md
- Data quality checks: ../reference/checks-data-quality.md
- Table options: ../reference/table-options.md
