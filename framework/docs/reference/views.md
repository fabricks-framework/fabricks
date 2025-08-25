# Views

Views package reusable SQL logic you can reference across jobs.

- Location: Place them under your runtime, e.g., `fabricks/views`.
- Usage: Reference them directly in SQL of downstream jobs:
  ```sql
  select * from fabricks.views.dim_calendar
  ```
- Keep views small and well-documented; use them to standardize joins, filters, and derivations.