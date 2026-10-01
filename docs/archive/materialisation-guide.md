# Materialisation Guide

How to choose the right materialisation for your models. This project runs on Snowflake, so guidance here is specific to Snowflake behaviour.

For an introduction to the model layers and when each is used, see the [Modelling Guide](modelling-guide.md).

## Quick Reference

| Materialisation | Creates | Rebuild cost | When to use |
|-----------------|---------|-------------|-------------|
| `view` | SQL view | None (query-time) | Staging models, lightweight transforms |
| `materialized_view` | Snowflake materialised view | Auto-maintained by Snowflake | Staging models with expensive single-source transforms |
| `table` | Physical table | Full rebuild each run | Most modelling and reporting models |
| `incremental` | Physical table | Partial rebuild | Large datasets with clear "new data" logic |
| `ephemeral` | Nothing (CTE) | None | One-off intermediate logic, not needed in database |

## View

A view stores the SQL definition but no data. Every time someone queries the view, Snowflake runs the underlying SQL.

```sql
{{ config(materialized='view') }}

select
    key_column,
    cast(date_column as date) as date_column
from {{ ref('raw_source_table') }}
```

### When to use views

- **Staging models** — the default for staging; most staging models are views
- **Simple transformations** — column renames, type casts, basic filters
- **Small result sets** — where the query runs fast enough that caching isn't needed

### When to avoid views

- Complex joins or aggregations that are slow to compute
- Models queried frequently by dashboards (each query re-runs the SQL)
- Models with many downstream dependents (each dependent re-computes the view)

### Snowflake specifics

- Views in Snowflake are free to store — you only pay compute when they're queried
- Views always show the latest source data (no staleness)
- Snowflake can optimise view queries through its query planner, but complex stacked views (view on view on view) can hit performance limits

### Default in this project

Views are the default for `models/raw/` and `models/staging/` as configured in `dbt_project.yml`:

```yaml
staging:
  +materialized: view
```

You don't need to add `{{ config(materialized='view') }}` to staging models unless you want to be explicit.

## Materialised View

A materialised view stores query results like a table but Snowflake keeps it in sync with the source data automatically — no rebuild needed on each dbt run.

```sql
{{ config(materialized='materialized_view') }}

select
    key_column,
    cast(date_column as date) as date_column
from {{ ref('raw_source_table') }}
```

### When to use materialised views

- **Staging models with expensive single-source transforms** — e.g. filtering a very large source table or applying costly expressions; the materialised view caches the result and Snowflake keeps it in sync automatically
- **Staging models where the transform cost matters** — if the view logic is cheap (renames, casts), a regular view is fine; a materialised view only helps when there's real compute to avoid repeating

### When to avoid materialised views

- Any model with joins, window functions, subqueries, or UDFs — Snowflake materialised views don't support these
- Models that select from another view — the source must be a physical table (this means the upstream raw model would need to be a table, not a view)
- Models where you need full control over when data refreshes

### Snowflake limitations

Snowflake materialised views are deliberately restrictive. They must:

- Select from a **single physical table** (not a view, not a join)
- Use only simple expressions — no window functions, no subqueries, no UDFs, no UNION
- Not use `GROUP BY` (aggregations are not supported)

These constraints happen to align well with what staging models typically do: select from one source, rename columns, cast types, and apply basic filters.

### Cost considerations

- Snowflake charges for the background maintenance process that keeps materialised views in sync
- For infrequently changing source data, this cost is low
- For very high-volume source tables that change constantly, the maintenance cost may outweigh the benefit — a regular table with a scheduled rebuild may be cheaper

### Materialised views vs tables for staging

| Consideration | Materialised view | Table |
|---------------|-------------------|-------|
| Stays fresh automatically | Yes | No — requires dbt run |
| Supports joins/window functions | No | Yes |
| Source must be a physical table | Yes | No |
| Rebuild on each run | No | Yes |
| Good for deduplication | No | Yes |
| Storage cost | Yes (auto-managed) | Yes |

Use a **materialised view** when the staging model is simple (single source, no joins) and freshness matters. Use a **table** when the staging model needs deduplication, joins, or other complex processing.

## Table

A table stores the query results as a physical table in Snowflake. Each `dbt run` drops and recreates the table.

```sql
{{ config(materialized='table') }}

select
    patient_id,
    count(*) as encounter_count
from {{ ref('stg_encounters') }}
group by patient_id
```

### When to use tables

- **Modelling and reporting models** — the default for anything below staging
- **Staging models with expensive processing** — deduplication, complex joins, or heavy transformations that would be too costly to recompute as a view
- **Complex transformations** — joins, window functions, heavy aggregations
- **Models with many downstream dependents** — compute once, read many times

### When to avoid tables

- Simple staging models (use views instead — tables are fine for staging models that do deduplication or heavy processing)
- Very large datasets where only new records change (consider incremental)
- Throwaway intermediate logic only used by one downstream model (consider ephemeral)

### Snowflake specifics

- Tables consume storage — you pay for the data at rest
- Snowflake's micro-partition pruning makes table scans efficient when you use `cluster_by`
- Tables are rebuilt from scratch on each run, which can be slow for very large datasets

### Clustering

For large tables, use `cluster_by` to tell Snowflake how to organise the data on disk. This dramatically improves query performance when filtering on the clustered columns:

```sql
{{ config(
    materialized='table',
    cluster_by=['practice_code']
) }}
```

Choose clustering columns based on how the table is typically queried — usually the most common filter or join keys. Good candidates:

- `practice_code` — for organisation-level analysis
- `analysis_month` — for time-series queries
- `person_id` — for patient-level lookups

Don't over-cluster. One or two columns is usually enough.

### Default in this project

Tables are the default for `models/modelling/` and `models/reporting/`:

```yaml
modelling:
  +materialized: table
reporting:
  +materialized: table
```

## Incremental

Incremental models only process **new or changed records** instead of rebuilding the entire table. This is much faster for large datasets.

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

select
    id,
    person_id,
    event_date,
    result_value
from {{ ref('stg_observations') }}
where result_value is not null

{% if is_incremental() %}
    and lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
```

### How it works

1. **First run** — builds the full table (same as a normal table)
2. **Subsequent runs** — only processes rows matching the `is_incremental()` filter
3. **Merge** — new rows are inserted; existing rows (matched by `unique_key`) are updated
4. **Full refresh** — `dbt run --full-refresh` rebuilds from scratch

### When to use incremental

- Large tables (millions of rows) where only a fraction of data changes
- Event logs, time-series data, or observation records with a clear timestamp
- Tables that take too long to rebuild fully on each run

### When to avoid incremental

- Small tables — the complexity isn't worth it
- Tables where the underlying logic changes frequently (each logic change needs a full refresh)
- Tables where you can't reliably identify "new" records

### Incremental Strategies on Snowflake

| Strategy | Behaviour | Best for |
|----------|-----------|----------|
| `merge` (default) | Insert new rows, update existing (by `unique_key`) | Records that can be updated after initial insert |
| `append` | Insert only, no updates | Immutable event logs |
| `delete+insert` | Delete matching rows then insert | When merge causes issues with complex keys |

### Key Configuration Options

```sql
{{ config(
    materialized='incremental',
    unique_key=['person_id', 'analysis_month'],  -- composite key
    incremental_strategy='merge',
    on_schema_change='fail',                      -- fail if columns change
    cluster_by=['analysis_month']                  -- performance optimisation
) }}
```

| Option | Purpose |
|--------|---------|
| `unique_key` | Column(s) to match existing rows. Required for merge/delete+insert. |
| `incremental_strategy` | How to handle new data (merge, append, delete+insert) |
| `on_schema_change` | What to do if columns change: `fail`, `append_new_columns`, `ignore`, `sync_all_columns` |
| `cluster_by` | Snowflake clustering for query performance |

### The `is_incremental()` Block

The `{% if is_incremental() %}` block runs only on incremental runs (not the first run, not full refreshes). Use it to filter for new data:

```sql
{% if is_incremental() %}
    -- Only process records newer than what we already have
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

`{{ this }}` refers to the current model's existing table in the database.

### Choosing Your Incremental Column

Pick a column that reliably identifies new or changed records:

| Column type | Example | Notes |
|------------|---------|-------|
| Processing timestamp | `lds_start_datetime` | Best choice — catches late-arriving data |
| Event timestamp | `created_at` | Good for append-only data |
| Updated timestamp | `updated_at` | Good for mutable records |
| Business date | `analysis_month` | Good for periodic data |

### Full Refresh

A full refresh rebuilds the entire table from scratch, ignoring the `is_incremental()` filter. This catches any data that slipped through the incremental filter (e.g. late-arriving records, retroactive corrections).

```bash
dbt run -s model_name --full-refresh
```

In production, the entire project runs with `--full-refresh` on the 1st of every month (see [Project Schedule](#project-schedule)). Always test your model with `--full-refresh` after changing its logic to ensure the full and incremental paths produce the same results.

### Example from This Project

Blood pressure observations using merge strategy with a processing timestamp:

```sql
{{ config(
    materialized='incremental',
    unique_key=['id', 'source_cluster_id'],
    incremental_strategy='merge',
    cluster_by=['person_id', 'effective_date']
) }}

with base_observations as (
    select
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.lds_start_datetime,
        obs.result_value,
        obs.mapped_concept_code as concept_code
    from ({{ get_observations("'BP_COD', 'SYSBP_COD', 'DIASBP_COD'") }}) obs
    where obs.result_value is not null
      and obs.person_id is not null
    {% if is_incremental() %}
      and obs.lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
    {% endif %}
)

select
    id,
    person_id,
    clinical_effective_date as effective_date,
    lds_start_datetime,
    result_value,
    concept_code,
    (concept_code = 'SYSBP_COD') as is_systolic_row,
    (concept_code = 'DIASBP_COD') as is_diastolic_row
from base_observations
```

## Ephemeral

Ephemeral models don't create any database object. Instead, dbt inlines them as CTEs in the downstream model that references them.

```sql
{{ config(materialized='ephemeral') }}

select
    person_id,
    max(event_date) as latest_event_date
from {{ ref('stg_events') }}
group by person_id
```

When a downstream model uses `{{ ref('this_ephemeral_model') }}`, dbt inserts the SQL as a CTE rather than querying a table or view.

### When to use ephemeral

- Intermediate logic used by a single downstream model
- Simple helper queries you don't need to inspect in the database
- Reducing the number of objects in Snowflake when the intermediate result has no standalone value

### When to avoid ephemeral

- When you need to query or debug the intermediate results directly
- When multiple downstream models reference the same ephemeral model (the SQL is duplicated in each)
- When the logic is complex enough that you'd want to test it independently
- When you want the model to appear in dbt docs lineage graphs (ephemeral models are hidden)

### Trade-offs

| Consideration | Ephemeral | View | Table |
|--------------|-----------|------|-------|
| Database object created | No | Yes | Yes |
| Queryable in Snowflake | No | Yes | Yes |
| Visible in lineage | No | Yes | Yes |
| Can be tested independently | No | Yes | Yes |
| Storage cost | None | None | Yes |

In practice, ephemeral models are rarely used in this project. Most intermediate logic either lives as CTEs within a single model file or as a separate `int_` model materialised as a table.

## Choosing the Right Materialisation

```
Is it a staging model?
  ├─ Yes, simple transforms → view (or materialised view if the transform is expensive)
  └─ Yes, but needs deduplication or heavy processing → table

Is the result set small (< 1 million rows)?
  └─ Yes → table

Is the table large AND does it have a clear "new data" timestamp?
  └─ Yes → incremental

Is it throwaway logic used by exactly one downstream model?
  └─ Yes → Consider ephemeral (but a CTE in the downstream model is usually simpler)

Everything else → table
```

### Summary by Layer

| Layer | Default | Override when... |
|-------|---------|------------------|
| Raw | View | Never — these are auto-generated |
| Staging | View | Use table for deduplication or expensive processing; consider materialised view for expensive single-source transforms |
| Modelling | Table | Use incremental for very large observation/event tables |
| Reporting | Table | Use incremental for large time-series fact tables |
| Published | Table | Never — published data should always be fully rebuilt |

## Project Schedule

The production build schedule affects how materialisations behave:

| When | What runs | Effect on materialisations |
|------|-----------|---------------------------|
| **Daily** | `dbt snapshot` | All snapshots capture changes |
| **Daily** | `dbt build -s +tag:daily` | Daily-tagged models (staging + daily-tagged downstream tables) and their upstream dependencies build; incremental models process only new data |
| **Monday** | `dbt build -s +staging+` | Staging, its raw ancestors, and everything downstream rebuild; incremental models process only new data |
| **1st of month** | `dbt build -s +staging+ --full-refresh` | Same scope, but all tables rebuilt from scratch, including incremental models |

Raw is not tagged `daily`; each schedule rebuilds only the raw views consumed by the staging models in scope (~166 of ~660), not every raw view. Raw views reflect live source on query and otherwise rebuild on deploy when their DDL changes. Models outside the staging lineage (`int_date_spine`, `def_indicator*`) build daily or on deploy, not on the Monday/monthly schedules.

### What This Means for You

- **Tables** behave the same regardless of schedule — they're rebuilt from scratch every time they run
- **Incremental models** process only new data on daily/weekly runs, but get a clean rebuild monthly
- **Views** are never "stale" — they always reflect the latest source data when queried
- **Snapshots** run daily to capture any changes before the main build

### Tagging Models for the Daily Build

If your model needs to run daily, add the `daily` tag:

```sql
{{ config(materialized='table', tags=['daily']) }}
```

Or in `dbt_project.yml` at the folder level (already configured for most layers). Models without the `daily` tag will only run on the Monday full build.

## Further Reading

- [Modelling Guide](modelling-guide.md) — layer principles and naming conventions
- [Snapshots Guide](snapshots-guide.md) — tracking historical changes with slowly changing dimensions
- [Development Guide](development-guide.md) — daily workflows and dbt commands
- [dbt Documentation: Materializations](https://docs.getdbt.com/docs/build/materializations)
