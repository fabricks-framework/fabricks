# Schedules

Views provide a simple, declarative way to define a set of jobs to run in a schedule. Instead of listing jobs manually, you create a SQL view that filters the canonical `fabricks.jobs` table, and then reference that view by name from a schedule.

Typical use cases:
- Run all jobs for a given domain/topic (e.g., monarch)
- Run only certain steps (e.g., all Gold jobs)
- Run a curated subset of jobs for ad‑hoc backfills or smoke tests

---

## Types of views in Fabricks

- Data views (user‑authored):
  - You place `.sql` files under your runtime `views` path (see below).
  - Each file defines one SQL view created under the `fabricks` schema.
  - These are typically simple filters over `fabricks.jobs`.

- Schedule views (framework‑generated):
  - For each `schedule` defined in your runtime YAML, Fabricks can generate a companion view named `fabricks.<schedule_name>_schedule`.
  - A schedule view selects `j.*` from `fabricks.jobs` and applies optional filters from the schedule’s options (`view`, `steps`, `tag`), excluding manual jobs.

Both kinds of views are useful: data views define job subsets; schedule views expose the final, resolved set for each schedule.

---

## How it works

- Source of truth: Fabricks maintains `fabricks.jobs` with one row per job.
- Data views: Your SQL selects a subset from `fabricks.jobs` (recommended: `select *`).
- Schedules:
  - In `schedule.yml`, you can set `options.view: <data_view_name>`.
  - Fabricks resolves the schedule’s membership via the data view (and optional `steps` / `tag` filters).
  - Fabricks can also materialize a schedule view `fabricks.<schedule_name>_schedule` for inspection and tooling.

---

## Where to put data view SQL files

The runtime configuration defines where Fabricks looks for view SQL files:

```yaml title:conf.fabricks.yml (excerpt)
path_options:
  views: fabricks/views
```

- Place your `.sql` files under the runtime `views` directory (e.g., `.../fabricks/views/`).
- Each file defines one view; the file name (without `.sql`) becomes the view name.
- During initialization, Fabricks will create or replace these as SQL views in the `fabricks` schema.

---

## Minimal data view example

Create a file `monarch.sql` in your runtime `views` directory:

```sql title:monarch.sql
-- Select all jobs with a specific topic
select *
from fabricks.jobs j
where j.topic = 'monarch'
```

This will create a view called `fabricks.monarch`. You can then reference this view in a schedule.

---

## Using a data view in a schedule

```yaml
- schedule:
    name: run-monarch
    options:
      view: monarch            # join to fabricks.monarch on job_id
      steps: [silver, gold]    # optional: restrict to these steps
      tag: nightly             # optional: only jobs having this tag
      # variables, timeouts, etc. can still be provided as needed
```

Behavior:
- Fabricks loads all jobs returned by `fabricks.monarch` and further filters by `steps` and `tag` if provided.
- Schedule‑level options like `variables`, `timeouts`, etc., still apply to execution.
- In most cases the view alone defines the job set; `steps`/`tag` refine it.

---

## Schedule views (generated)

For each schedule, Fabricks can generate a companion SQL view named `fabricks.<schedule_name>_schedule` that reflects the resolved membership. The core SQL shape (simplified) is:

```sql
create or replace view fabricks.<schedule_name>_schedule as
select j.*
from fabricks.jobs j
/* optional: if options.view is provided */
inner join fabricks.<options.view> v on j.job_id = v.job_id
where true
  /* optional: if options.steps is provided */
  and j.step in ('bronze','silver','gold' /* ... */)
  /* optional: if options.tag is provided (array of tags on the job) */
  and array_contains(j.tags, '<tag>')
  and j.type not in ('manual');
```

Notes:
- The inner join requires the data view to expose `job_id`. Returning `j.*` from `fabricks.jobs` guarantees this.
- Manual jobs are excluded from schedule views by design.

Quick validation:
```sql
select count(*) from fabricks.run-monarch_schedule;         -- schedule membership size
select step, topic, item from fabricks.run-monarch_schedule limit 20;
```

---

## Registration and lifecycle

You can (re)create both data views and schedule views programmatically.

- Data views (from `.sql` files under `path_options.views`):
  ```python
  from fabricks.core.views import (
      create_or_replace_view,   # single data view by name
      create_or_replace_views,  # all data views under PATH_VIEWS
  )

  create_or_replace_view("monarch")
  create_or_replace_views()
  ```

- Schedule views (one per schedule defined in runtime):
  ```python
  from fabricks.core.schedules import (
      create_or_replace_view as create_or_replace_schedule_view,  # single schedule by name
      create_or_replace_views as create_or_replace_schedule_views # all schedules
  )

  create_or_replace_schedule_view("run-monarch")
  create_or_replace_schedule_views()
  ```

- Initialization path:
  - The bootstrap script (“armageddon”) calls both data view and schedule view builders so your environment is in sync with runtime configs and SQL view files.

---

## Additional data view examples

Filter by step:
```sql title:gold_only.sql
select *
from fabricks.jobs j
where j.step = 'gold'
```

Filter by a set of topics:
```sql title:core_topics.sql
select *
from fabricks.jobs j
where j.topic in ('sales', 'finance', 'marketing')
```

Filter by both step and topic pattern:
```sql title:gold_sales_like.sql
select *
from fabricks.jobs j
where j.step = 'gold'
  and j.topic like 'sales_%'
```

Curate explicit jobs:
```sql title:curated_selection.sql
select *
from fabricks.jobs j
where (j.step = 'silver' and j.topic = 'monarch' and j.item = 'scd1__current')
   or (j.step = 'gold'   and j.topic = 'monarch' and j.item = 'orders')
```

Tip:
- You may project specific columns, but for schedules that use `options.view`, ensure `job_id` is present in the projection. Returning `j.*` is simplest.

---

## Best practices

- Keep view logic simple: use filters, avoid heavy joins or transformations.
- Align views with business domains or execution scopes (by step, topic family, tags).
- Use explicit OR lists for one‑off backfills to keep intent clear and auditable.
- Favor stable view names; schedules reference the view name.

## Related topics

- Initialization and bootstrap: ../helpers/init.md
- Job helper and operations: ../helpers/job.md
- Steps overview (Bronze/Silver/Gold): ../steps/gold.md and siblings
- Data quality checks: ./checks-data-quality.md
- Table options and storage layout: ./table-options.md
