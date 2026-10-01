# What is dbt?

An introduction to dbt for anyone joining this project. If you already know dbt and want to get started, jump straight to [CONTRIBUTING.md](../CONTRIBUTING.md) or the [Modelling Guide](modelling-guide.md).

## The Problem

Healthcare analytics teams typically work with SQL. Analysts write queries to extract, clean, and transform data from source systems into the tables and views that power dashboards and reports.

Over time this creates problems:

- **No version control** — SQL scripts live on shared drives, in personal folders, or inside BI tools. Nobody knows which version is current, and changes are hard to trace
- **No dependency management** — queries rely on other queries, but those relationships are invisible. Changing one table can silently break downstream reports
- **No testing** — there's no systematic way to check that a query still produces correct results after a change
- **No documentation** — knowledge about what a query does, what columns mean, and what business rules are applied lives in people's heads
- **Manual orchestration** — someone has to remember what to run and in what order. If a step is missed, data is stale or wrong
- **Copy-paste reuse** — common logic (age calculations, date handling, code lookups) is duplicated across dozens of scripts with slight variations

This project used to face these exact problems. dbt solves them.

## What dbt Is

**dbt (data build tool)** is an open-source framework that lets you transform data using SQL, while providing the engineering practices that SQL on its own lacks — version control, testing, documentation, and dependency management.

dbt sits between your data warehouse and your analysts:

```
Source Systems → Data Warehouse → dbt → Analytics-ready tables → Dashboards/Reports
         (Snowflake)
```

You write SQL `SELECT` statements. dbt handles everything else: creating tables, managing dependencies, running tests, and generating documentation.

### What dbt is not

- **Not an ETL/ELT tool** — dbt doesn't extract data from source systems or load it into your warehouse. It only transforms data that's already there
- **Not a BI tool** — dbt produces tables and views, not charts or dashboards
- **Not a database** — dbt runs against your existing data warehouse (Snowflake in our case)

## How dbt Works

### Models

A dbt **model** is a SQL `SELECT` statement saved as a `.sql` file. When you run dbt, it wraps your `SELECT` in a `CREATE TABLE` or `CREATE VIEW` statement and executes it against the database.

```sql
-- models/staging/shared/dictionary/stg_dictionary_dbo_dates.sql
select
    sk_date_id,
    cast(full_date as date) as full_date,
    day_of_week,
    financial_year
from {{ ref('raw_dictionary_dbo_dates') }}
```

You write the `SELECT`. dbt decides how to materialise it (as a table, view, etc.) based on configuration.

### References

The `{{ ref() }}` function is how models reference each other. Instead of hardcoding table names:

```sql
-- Bad: hardcoded reference
select * from STAGING.DICTIONARY.stg_dictionary_dbo_dates

-- Good: dbt reference
select * from {{ ref('stg_dictionary_dbo_dates') }}
```

`ref()` does two things:
1. Resolves to the correct database and schema (different in dev vs prod)
2. Tells dbt that this model depends on `stg_dictionary_dbo_dates`, so it must be built first

This is how dbt builds a **dependency graph** (DAG) — it reads every `ref()` call and works out the correct build order automatically. No manual orchestration needed.

### Sources

The `{{ source() }}` function references tables that exist outside dbt (in the data lake). Sources are defined in YAML and only used in the raw layer:

```sql
select * from {{ source('dictionary_dbo', 'Dates') }}
```

### Tests

dbt tests are assertions about your data. They're defined in YAML alongside model documentation:

```yaml
columns:
  - name: sk_patient_id
    tests:
      - unique
      - not_null
```

When you run `dbt test`, dbt generates a SQL query for each assertion and checks that it passes. If `sk_patient_id` has duplicates or nulls, the test fails and you know about it before the data reaches a dashboard.

### Documentation

Every model and column can be documented in YAML files that live alongside the SQL:

```yaml
models:
  - name: dim_practice
    description: Practice-level dimension table with current attributes
    columns:
      - name: practice_code
        description: ODS practice code (e.g. A12345)
```

Running `dbt docs generate && dbt docs serve` produces a searchable documentation site with a full lineage graph showing how data flows through the project.

## Where dbt Adds Value

### 1. Version control and collaboration

Every model is a file in Git. Changes go through pull requests with code review. You can see exactly what changed, when, and why. Multiple people can work on different models simultaneously without stepping on each other.

### 2. Dependency management

dbt reads `ref()` calls and builds a directed acyclic graph (DAG) of your entire pipeline. Run `dbt build` and models execute in the correct order. Change an upstream model and dbt knows which downstream models to rebuild:

```bash
dbt run -s +dim_practice    # Builds dim_practice and everything it depends on
dbt run -s dim_practice+    # Builds dim_practice and everything that depends on it
```

### 3. Testing

Tests catch data quality issues before they reach reports:

```bash
dbt test                     # Run all tests
dbt test -s model_name       # Test a specific model
```

Tests run automatically in CI on every pull request (see [GitHub Actions](../github-actions.md)). A failing test blocks the PR from merging.

### 4. Documentation

Documentation lives next to the code it describes, so it stays up to date. The generated docs site includes:
- Model and column descriptions
- A full lineage graph (visual DAG)
- Source freshness information
- Test coverage

### 5. Environment separation

The same code runs in development and production. dbt handles the differences:

| Environment | Database prefix | Example |
|-------------|----------------|---------|
| Development | `DEV__` | `DEV__STAGING.PDS.stg_pds_person` |
| Production | *(none)* | `STAGING.PDS.stg_pds_person` |

You develop and test against `DEV__` databases. When your PR is merged, the deployment pipeline runs the same code against production.

### 6. DRY code with macros

Common logic is written once as a macro and reused everywhere:

```sql
-- Instead of copy-pasting age calculations everywhere:
{{ calculate_age('date_of_birth', 'event_date') }}
```

If the logic needs to change, you update one file and every model that uses it picks up the change.

### 7. Incremental processing

For large tables, dbt can process only new or changed records instead of rebuilding from scratch:

```sql
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

This makes daily refreshes practical even for tables with millions of rows.

## How This Relates to This Project

This dbt project transforms healthcare data from the data lake into the analytical datasets that power dashboards and reports across the ICB. The data flows through five layers:

```
DATA_LAKE → Raw → Staging → Modelling → Reporting → Published
```

Each layer is a folder in the `models/` directory. The [Modelling Guide](modelling-guide.md) explains each layer in detail.

### Before dbt

- SQL scripts in various locations with no version history
- Manual dependency tracking — analysts had to know what to run and in what order
- No automated testing — data issues were discovered when dashboards looked wrong
- Documentation in separate documents that went stale quickly

### With dbt

- All transformation logic in a single Git repository
- Automated dependency resolution and build ordering
- Tests run on every pull request and every production deployment
- Documentation generated directly from the code
- CI/CD pipeline validates changes before they reach production
- Clear separation between dev and prod environments

## Getting Started

1. **Set up your environment** — follow [CONTRIBUTING.md](../CONTRIBUTING.md)
2. **Understand the layers** — read the [Modelling Guide](modelling-guide.md)
3. **Learn the daily workflow** — see the [Development Guide](development-guide.md)
4. **Use the dbt Fusion extension** — see the [dbt Fusion Guide](dbt-fusion-guide.md) for IDE features

For general dbt learning beyond this project:
- [dbt Fundamentals (VS Code)](https://learn.getdbt.com/courses/dbt-fundamentals-vs-code) — free introductory course
- [dbt Learn catalog](https://learn.getdbt.com/catalog) — full course library
- [dbt Documentation](https://docs.getdbt.com/) — official reference
- [dbt Community Slack](https://www.getdbt.com/community/) — community support
