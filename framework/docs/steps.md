# Steps Overview

Fabricks organizes your Lakehouse into clear layers. Each step has a dedicated reference with modes, options, and examples.

---

## Bronze
Raw ingestion from source systems (files, streams, existing tables).
- Typical modes: memory, append, register
- Focus: lightweight parsing/landing; no business logic
- Output: raw tables or temporary views

Read the full reference → [Bronze Step](steps/bronze.md)

---

## Silver
Standardize, clean, and enrich data; optionally apply CDC.
- Typical modes: memory, append, latest, update, combine
- CDC: nocdc, scd1, scd2 with built-in helpers and views
- Output: conformed tables and curated views

Read the full reference → [Silver Step](steps/silver.md)

---

## Gold
Curated business models for analytics and reporting.
- Typical modes: memory, append, complete, update, invoke (notebooks)
- Focus: dimensional models, marts, KPI-ready data
- Output: business-consumption tables and views

Read the full reference → [Gold Step](steps/gold.md)

---

## Semantic
Apply table properties/metadata and optional copy operations.
- Mode: complete (apply properties/metadata/copy)
- Use to ensure consistent governance, layout, and table features

Read the full reference → [Semantic Step](steps/semantic.md)
