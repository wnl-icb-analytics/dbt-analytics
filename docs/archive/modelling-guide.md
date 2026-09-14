# Modelling Guide

A practical guide to building models in this project. If you haven't set up your environment yet, start with [CONTRIBUTING.md](../CONTRIBUTING.md).

## How Data Flows Through the Project

```
DATA_LAKE (external sources)
    │
    ▼
┌─────────┐   Auto-generated views that rename columns
│   Raw   │   from PascalCase to snake_case.
└────┬────┘   Don't edit these manually.
     │
     ▼
┌──────────┐  Clean and standardise: cast types, handle
│ Staging  │  nulls, rename columns. One model per source.
└────┬─────┘  No joins. No business logic.
     │
     ▼
┌───────────┐ Modular building blocks: joins, CTEs,
│ Modelling │ reusable components. Each model does
└────┬──────┘ one specific thing well.
     │
     ▼
┌───────────┐ Our marts layer. Combines modelling
│ Reporting │ components into clean, analytics-ready
└────┬──────┘ datasets for analysts to use directly.
     │
     ▼
┌───────────┐ Production datasets for dashboards
│ Published │ and end-user tools. Governance and
└───────────┘ access controls applied.
```

Each layer references the one above it using `{{ ref() }}`. Raw models reference sources using `{{ source() }}`.

## Raw Layer

**Location:** `models/raw/`
**Materialisation:** View
**Schema:** `STAGING.DBT_RAW`

Raw models are **auto-generated** by the Python scripts in `scripts/sources/`. Their only job is to create a 1:1 view of each source table with column names converted from PascalCase to snake_case.

**Example** (`raw_dictionary_dbo_consultantprovider.sql`):

```sql
select
    "SK_ConsultantID" as sk_consultant_id,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_SpecialtyID" as sk_specialty_id,
    "StartDate" as start_date,
    "EndDate" as end_date
from {{ source('dictionary_dbo', 'ConsultantProvider') }}
```

**Key rules:**
- Never edit raw model SQL files manually — they will be overwritten on the next generation run
- To add new raw models, follow the source generation workflow in [Working with Sources](../working-with-sources.md)
- Raw models have no YAML documentation files (the config block documents column mappings)
- They exist so that every other layer can work with clean, consistent snake_case column names

## Staging Layer

**Location:** `models/staging/{domain}/{source}/`
**Materialisation:** View
**Schema:** `STAGING.{SOURCE}` — one schema per source system, named after the folder containing the model (e.g. `models/staging/commissioning/csds/` → `STAGING.CSDS`)

The staging layer is where you clean and standardise source data. Every staging model takes a single raw model and applies lightweight transformations to make the data usable.

### Principles

1. **One model per source** — each staging model maps to exactly one raw model
2. **No joins** — never join to other tables in staging; that belongs in modelling
3. **No business logic** — no aggregations, no case-when business rules, no derived metrics
4. **Data cleaning only:**
   - Cast data types (`cast(full_date as date)`, `to_date(year_month_of_birth, 'YYYYMM')`)
   - Handle nulls (`where extract_date is not null`)
   - Rename columns for clarity (`gender as gender_code`)
   - Select columns explicitly (no `select *`)
5. **Every model must have a YAML file** with description, owner, and tests

### Naming Convention

```
stg_{source}_{table}.sql
stg_{source}_{table}.yml
```

For example: `stg_pds_person.sql` stages the `raw_pds_pds_person` raw model.

### Example SQL

```sql
-- stg_pds_person.sql
select
    row_id,
    pseudo_nhs_number as sk_patient_id,
    to_date(year_month_of_birth, 'YYYYMM') as year_month_of_birth,
    gender as gender_code,
    to_date(date_of_death) as date_of_death,
    death_status,
    preferred_language as preferred_language_code,
    interpreter_required,
    to_date(person_business_effective_from_date) as event_from_date,
    to_date(person_business_effective_to_date) as event_to_date

from {{ ref('raw_pds_pds_person') }}
```

Notice: type conversions with `to_date()`, meaningful column renames, explicit column selection, no joins.

### Example YAML

```yaml
version: 2

models:
  - name: stg_pds_person
    description: Personal Demographics Service person demographics
    config:
      meta:
        owner:
          name: EddieDavison92
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - row_id
            - sk_patient_id
    columns:
      - name: row_id
        description: Row identifier
        tests:
          - not_null
      - name: sk_patient_id
        description: Surrogate key patient identifier (pseudonymised NHS number)
        tests:
          - not_null
      - name: year_month_of_birth
        description: Year and month of birth
      - name: date_of_death
        description: Date of death
```

### What to Test in Staging

| Test | When to use |
|------|-------------|
| `not_null` | Primary keys and essential columns |
| `unique` | Natural keys and identifiers |
| `dbt_utils.unique_combination_of_columns` | Composite keys |
| `dbt_expectations.expect_table_row_count_to_be_between` | Ensure table isn't empty |

### Generating YAML Scaffolding

Use the codegen package to generate a starting point for your YAML:

```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["stg_your_model"], "upstream_descriptions": true}'
```

This outputs a YAML template to your terminal. Copy it into a `.yml` file alongside your SQL, then add descriptions, owner, and tests.

## Modelling Layer

**Location:** `models/modelling/`
**Materialisation:** Table
**Database:** `MODELLING`

The modelling layer is for building **modular, reusable components**. Each model should do one specific thing well — filter a cohort, join two datasets, calculate a set of flags — so it can be composed into reporting models downstream.

Think of modelling models as building blocks. A dialysis encounters model identifies dialysis visits. A borough mapping model resolves geographic hierarchies. A blood pressure observations model extracts and classifies BP readings. Each is focused and testable on its own.

### Principles

1. **Keep each model focused** — one model, one job. If a model is doing too many things, split it up
2. **Use CTEs for clarity** — break complex logic within a model into named steps
3. **Join freely** — combine data from multiple staging models
4. **Use macros** — leverage project macros for common patterns (age calculations, code cleaning, etc.)
5. **Document with YAML** — descriptions, owner, and tests

### Domain Organisation

Models are organised by domain and subdomain:

```
models/modelling/
├── commissioning/
│   ├── activities/        # Activity-based analysis
│   ├── diagnosis/         # Diagnostic groupings
│   ├── encounters/        # Patient encounters
│   └── observations/      # Clinical observations
└── olids/
    ├── diagnoses/         # QOF disease registers
    ├── medications/       # Prescribing data
    ├── observations/      # Clinical observations
    ├── organisation/      # Practice hierarchies
    ├── person_attributes/ # Patient demographics
    └── programme/         # Clinical programmes
```

For the `olids` domain, Snowflake schema names are auto-derived from the folder structure. For example, `models/modelling/olids/diagnoses/` maps to `MODELLING.OLIDS_DIAGNOSES`. Simply create a new subfolder and the schema is generated automatically.

### Naming Convention

Use the `int_` prefix for intermediate models:

```
int_{domain}_{description}.sql
```

For example: `int_activity_dialysis.sql`, `int_organisation_borough_mapping.sql`.

### Example

```sql
-- int_activity_dialysis.sql
with specialty_filters as (
    select visit_occurrence_id
    from {{ ref('int_observations') }}
    where
        (observation_vocabulary = 'HRG'
            and observation_concept_code in ('LA08E', 'LE01A', 'LE01B', 'LE02A', 'LE02B'))
        or
        (observation_vocabulary = 'OPCS4'
            and observation_concept_code like 'X40%')
),

dialysis_encounters as (
    select distinct visit_occurrence_id
    from specialty_filters
)

select
    e.visit_occurrence_id,
    e.start_date,
    e.organisation_id,
    e.organisation_name,
    e.main_specialty_code,
    e.core_hrg_code
from {{ ref('int_sus_op_appointments') }} e
inner join dialysis_encounters m
    on e.visit_occurrence_id = m.visit_occurrence_id
```

Notice: the model does one thing — identifies dialysis encounters. It doesn't also aggregate them or join in patient demographics. Those are separate models downstream.

## Reporting Layer

**Location:** `models/reporting/`
**Materialisation:** Table
**Database:** `REPORTING`

The reporting layer is our **marts layer** — it produces clean, well-structured datasets that analysts can query directly for ad-hoc analysis, exploration, and development work. Think of reporting as the curated analytical layer: it assembles modelling components into meaningful, documented datasets with proper business logic applied.

Reporting models can and do contain business logic — QOF disease register calculations, clinical programme definitions, and complex eligibility criteria all live here. The difference from modelling is purpose: modelling builds focused, reusable components; reporting assembles them into complete analytical datasets.

**Important:** Dashboards and end-user tools do **not** query reporting models directly. For a dataset to be consumed by a dashboard, it must first be promoted to the [Published layer](#published-layer), where governance controls and access policies are applied. Reporting is for **analysts** — published is for **dashboards and production consumers**.

### Common Patterns

**Fact tables** — one row per entity with aggregated metrics across time windows:

```sql
select
    sk_patient_id,
    count(distinct case
        when start_date between dateadd(month, -3, current_date())
            and current_date()
        then visit_occurrence_id
    end) as encounters_3mo,
    count(distinct visit_occurrence_id) as encounters_12mo,
    sum(duration) as total_los_12mo
from {{ ref('int_sus_apc_encounters') }}
where start_date between dateadd(month, -12, current_date()) and current_date()
group by sk_patient_id
```

**Dimension tables** — descriptive attributes joined from multiple sources (practice details, patient demographics, geographic hierarchies).

**Register and programme tables** — combine modelling components to determine patient eligibility, calculate QOF registers, or aggregate programme metrics.

### Naming Convention

| Prefix | Use |
|--------|-----|
| `dim_` | Dimension tables (descriptive attributes) |
| `fct_` | Fact tables (metrics and measures) |

### Subdomain Organisation

Like modelling, reporting models are organised by subdomain:

```
models/reporting/
├── commissioning/
│   ├── person_level/      # Patient-level aggregations
│   ├── person_history/    # Historical views
│   └── provider_level/    # Provider-level aggregations
└── olids/
    ├── disease_registers/ # QOF disease register analytics
    ├── measures/          # Clinical quality measures
    ├── organisation/      # Practice-level reporting
    └── programme/         # Programme-level reporting
```

## Published Layer

**Location:** `models/published/`
**Materialisation:** Table

The published layer is the **production-facing layer** — this is what dashboards, Power BI reports, and other end-user tools query. No dashboard should reference a reporting model directly; it must go through published first.

Published models take reporting datasets and apply the necessary governance controls, access policies, and privacy filters before exposing them to consumers. They are split into two databases based on data governance requirements:

| Database | Purpose | Access |
|----------|---------|--------|
| `PUBLISHED_REPORTING__DIRECT_CARE` | Individual patient care data | Consent-based access |
| `PUBLISHED_REPORTING__SECONDARY_USE` | Population health analytics | Opt-out filtering applied |

Secondary use models typically apply a privacy filter by joining to an allowed-persons table:

```sql
select base.*
from {{ ref('population_health_needs_base') }} base
inner join {{ ref('dim_person_secondary_use_allowed') }} allowed
    on base.person_id = allowed.person_id
```

#### Section 251 exempt dashboards

A small number of secondary use dashboards are published under a **Section 251 exemption** as HealtheIntent replacement dashboards (population health needs, childhood immunisations, COVID/flu, LTC LCS case finding and risk stratification, valproate). These are still secondary use, but opt-out filtering is **intentionally not applied** — so they do **not** join to `dim_person_secondary_use_allowed`. They are passthroughs of the direct care base:

```sql
select *
from {{ ref('population_health_needs_base') }}
```

Each exempt model overrides its `meta.custom_message` so the table comment states the exemption rather than claiming opt-outs were applied.

## Naming Conventions Summary

### File Names

| Layer | Pattern | Example |
|-------|---------|---------|
| Raw | `raw_{source}_{table}.sql` | `raw_dictionary_dbo_dates.sql` |
| Staging | `stg_{source}_{table}.sql` | `stg_dictionary_dbo_dates.sql` |
| Modelling | `int_{description}.sql` | `int_activity_dialysis.sql` |
| Reporting | `dim_` or `fct_{description}.sql` | `dim_practice.sql`, `fct_person_sus_apc_recent.sql` |
| Published | `{description}.sql` | `population_health_needs_base.sql` |

### Column Names

- Always use `snake_case`
- Use `sk_` prefix for surrogate keys (e.g. `sk_patient_id`)
- Use `_code` suffix for coded values (e.g. `gender_code`, `practice_code`)
- Use `_date` suffix for dates (e.g. `start_date`, `date_of_death`)
- Use `is_` or `has_` prefix for boolean flags (e.g. `is_systolic_row`, `has_acute_kidney_injury`)

## Writing Tests and Documentation

### CI Checks

Pull requests are automatically checked for:

| Check | Requirement |
|-------|------------|
| **Model descriptions** | All changed models must have a description in YAML |
| **Model tests** | All changed models must have at least one test |
| **Staging references** | Raw/source references must only appear in staging models |
| **Hardcoded references** | No hardcoded table or database names in SQL |

See [GitHub Actions](../github-actions.md) for full details.

### YAML File Structure

Every model (except raw) should have an accompanying `.yml` file:

```yaml
version: 2

models:
  - name: your_model_name
    description: Clear description of what this model does
    config:
      meta:
        owner:
          name: YourGitHubUsername
    columns:
      - name: primary_key_column
        description: What this column represents
        tests:
          - unique
          - not_null
      - name: another_column
        description: What this column represents
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
```

## Adding a New Model: Step by Step

Here's a practical walkthrough for adding a new staging model.

### 1. Identify the source

Check whether a raw model already exists for your source table. The quickest way is to use the **VS Code search bar** (`Ctrl+Shift+F`) and search for your source name within `models/raw/` — this is much faster than running a dbt command which requires compilation.

Alternatively, you can use the CLI:

```bash
dbt ls -s raw --output name | grep your_source
```

If a raw model doesn't exist, follow [Working with Sources](../working-with-sources.md) to generate it.

### 2. Create the staging SQL

Create `models/staging/{domain}/{source}/stg_{source}_{table}.sql`. The source folder name becomes the schema in `STAGING`, so add the model to the existing source folder or create a new one named after the source system:

```sql
select
    key_column,
    cast(date_column as date) as date_column,
    text_column
from {{ ref('raw_{source}_{table}') }}
where key_column is not null
```

### 3. Create the YAML

Create `models/staging/{domain}/{source}/stg_{source}_{table}.yml`:

```yaml
version: 2

models:
  - name: stg_{source}_{table}
    description: Describe what this data is and any transformations applied
    config:
      meta:
        owner:
          name: YourGitHubUsername
    columns:
      - name: key_column
        description: Primary key
        tests:
          - unique
          - not_null
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1
```

### 4. Build and test

```bash
dbt build -s stg_{source}_{table}
```

This runs the model and its tests in one command. Fix any test failures before pushing.

### 5. Preview your data

```bash
dbt show -s stg_{source}_{table}
```

This shows the first 5 rows without creating a database object — useful for checking your transformations look right.

## Further Reading

- [Materialisation Guide](materialisation-guide.md) — choosing between views, tables, incremental models, and ephemeral CTEs
- [Snapshots Guide](snapshots-guide.md) — tracking historical changes with slowly changing dimensions
- [Working with Sources](../working-with-sources.md) — the source generation workflow
- [Development Guide](development-guide.md) — daily workflows and advanced patterns
- [GitHub Actions](../github-actions.md) — CI/CD pipelines and deployment
