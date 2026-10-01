# Snapshots Guide

How to use dbt snapshots to track historical changes in source data. Snapshots implement [slowly changing dimension (SCD) Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) logic, preserving every version of a record over time.

For general model-building guidance, see the [Modelling Guide](modelling-guide.md). For choosing between materialisations, see the [Materialisation Guide](materialisation-guide.md).

## What Snapshots Do

Some source data changes over time without keeping history. For example, a patient's registered GP practice might update in place — once it changes, the old value is gone. Snapshots solve this by recording every version of a row with validity dates.

**Before snapshot** — only the current state:

| patient_id | practice_code |
|-----------|---------------|
| 123 | A12345 |

**After snapshot** — full history preserved:

| patient_id | practice_code | dbt_valid_from | dbt_valid_to |
|-----------|---------------|----------------|--------------|
| 123 | B67890 | 2024-01-15 | 2024-09-01 |
| 123 | A12345 | 2024-09-01 | *null* |

The row with `dbt_valid_to = null` is the current record.

## How Snapshots Work

1. **First run** — captures a full copy of the source data, adds `dbt_valid_from` (set to now) and `dbt_valid_to` (set to null)
2. **Subsequent runs** — compares each row against what's already stored:
   - **Unchanged rows** — left alone
   - **Changed rows** — the old version gets `dbt_valid_to` set to now; a new version is inserted with `dbt_valid_to = null`
   - **New rows** — inserted with `dbt_valid_to = null`
   - **Deleted rows** — optionally invalidated (depends on `invalidate_hard_deletes` setting)

Snapshots are always additive — they never delete history.

## Creating a Snapshot

Snapshot files live in the `snapshots/` directory.

### Timestamp Strategy

Use when the source table has an `updated_at` column:

```sql
{% snapshot snapshot_patient_registration %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='patient_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select * from {{ ref('stg_patient_registration') }}

{% endsnapshot %}
```

dbt compares the `updated_at` value to detect changes. Only rows where `updated_at` has increased since the last snapshot run are processed.

### Check Strategy

Use when the source table has no reliable timestamp:

```sql
{% snapshot snapshot_practice_details %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='practice_code',
        strategy='check',
        check_cols=['practice_name', 'status', 'borough']
    )
}}

select * from {{ ref('stg_practice_details') }}

{% endsnapshot %}
```

dbt compares the values of `check_cols` to detect changes. Use `check_cols='all'` to check every column (slower but catches everything).

### Configuration Options

| Option | Required | Description |
|--------|----------|-------------|
| `target_schema` | Yes | Snowflake schema where snapshot table is created |
| `unique_key` | Yes | Column(s) that uniquely identify each row |
| `strategy` | Yes | `timestamp` or `check` |
| `updated_at` | If timestamp | Column containing the last-updated timestamp |
| `check_cols` | If check | List of columns to compare, or `'all'` |
| `invalidate_hard_deletes` | No | Set `dbt_valid_to` on rows deleted from source (default: false) |

## Running Snapshots

```bash
dbt snapshot                          # Run all snapshots
dbt snapshot -s snapshot_name         # Run a specific snapshot
```

Snapshots are **not** included in `dbt build` or `dbt run` — you must run them explicitly with `dbt snapshot`.

### Scheduling

Snapshots run **daily** in production, after the main dbt build. See [Project Schedule](#project-schedule) below for the full schedule.

## Snapshot Columns

dbt automatically adds these metadata columns to snapshot tables:

| Column | Description |
|--------|-------------|
| `dbt_scd_id` | Unique identifier for each snapshot row |
| `dbt_updated_at` | When this snapshot row was created or last validated |
| `dbt_valid_from` | When this version of the record became active |
| `dbt_valid_to` | When this version was superseded (null = current) |

### Querying Snapshot Data

**Current state only** (equivalent to the source table):

```sql
select *
from {{ ref('snapshot_patient_registration') }}
where dbt_valid_to is null
```

**State at a specific point in time**:

```sql
select *
from {{ ref('snapshot_patient_registration') }}
where dbt_valid_from <= '2024-06-01'
  and (dbt_valid_to > '2024-06-01' or dbt_valid_to is null)
```

**Full history for a specific entity**:

```sql
select *
from {{ ref('snapshot_patient_registration') }}
where patient_id = '123'
order by dbt_valid_from
```

## When to Use Snapshots

**Good use cases:**
- Patient registration history (which practice, when)
- Practice metadata that changes (name, status, PCN membership)
- Reference data that evolves (code descriptions, organisational hierarchies)
- Any source table that updates in place without keeping its own history

**Avoid snapshots when:**
- The source already provides full history (event logs, audit tables)
- You only need the current state
- The source table is very large and changes frequently (consider incremental models instead)
- The data is immutable (event data, transactions)

## Snapshot vs Incremental

| Aspect | Snapshot | Incremental |
|--------|----------|-------------|
| **Purpose** | Track history of changing records | Efficiently process new records |
| **Creates history** | Yes — every version preserved | No — just appends/updates |
| **Source data** | Mutable (records change in place) | Growing (new records added) |
| **Location** | `snapshots/` directory | `models/` directory |
| **Run command** | `dbt snapshot` | `dbt run` / `dbt build` |
| **SCD columns** | Automatic (`dbt_valid_from`, `dbt_valid_to`) | Manual (if needed) |

## Best Practices

1. **Snapshot from staging models** — use `{{ ref('stg_...') }}` not `{{ source() }}` directly, so you benefit from cleaned column names and types

2. **Use timestamp strategy when possible** — it's faster than check because it only processes rows where the timestamp has changed

3. **Be selective with check_cols** — listing specific columns is faster and avoids false positives from metadata columns that change without meaning

4. **Don't snapshot large, frequently changing tables** — snapshots grow over time; a table with millions of rows changing daily will create a very large snapshot table

5. **Run snapshots before your main build** — so that downstream models can reference the latest snapshot data

6. **Never manually modify snapshot tables** — dbt manages the SCD logic; manual changes will corrupt the history

7. **Test your snapshots** — add tests to validate the snapshot data:

```yaml
version: 2

snapshots:
  - name: snapshot_patient_registration
    description: Historical record of patient practice registrations
    columns:
      - name: patient_id
        tests:
          - not_null
      - name: dbt_valid_from
        tests:
          - not_null
```

## Project Schedule

Snapshots run as part of the production build schedule:

| When | What runs |
|------|-----------|
| **Daily** | `dbt snapshot` — all snapshots capture changes |
| **Daily** | `dbt build -s +tag:daily` — daily-tagged models and their upstream dependencies build |
| **Monday** | `dbt build -s +staging+` — staging, its raw ancestors, and all downstream rebuild |
| **1st of month** | `dbt build -s +staging+ --full-refresh` — same scope, all tables rebuilt from scratch |

Snapshots always run before the main model build so that downstream models can reference the latest snapshot data.

## Further Reading

- [Modelling Guide](modelling-guide.md) — layer principles and naming conventions
- [Materialisation Guide](materialisation-guide.md) — views, tables, incremental, and ephemeral
- [dbt Documentation: Snapshots](https://docs.getdbt.com/docs/build/snapshots)
