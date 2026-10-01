# Development Guide

This guide covers advanced development patterns and technical details for working with the WNL ICB Analytics dbt project.

## Daily Development Workflow

### Running Models

Use `dbt build` to build models and run tests in one go:
```bash
dbt build                          # Build all models and run tests
dbt build -s model_name            # Build one model and test it
dbt build -s +model_name           # Build model with dependencies and test
dbt build -s olids                 # Build all OLIDS models and test
```

If you need to run and test separately:
```bash
dbt run                            # Build all models only
dbt test                           # Run tests only
```

Build specific models:
```bash
dbt run -s model_name              # Build one model
dbt run -s +model_name             # Build model + upstream dependencies
dbt run -s model_name+             # Build model + downstream dependents
dbt run -s +model_name+            # Build model + all dependencies
```

Build by folder/path:
```bash
dbt run -s staging                 # Build all staging models
dbt run -s modelling/commissioning # Build all commissioning modelling models
dbt run -s reporting/olids         # Build all OLIDS reporting models
dbt run -s modelling/olids/diagnoses  # Build specific subdomain
```

Build by tag:
```bash
dbt run -s tag:daily               # Build all models tagged 'daily'
dbt run -s tag:qof                 # Build all QOF-related models
dbt run -s tag:priority +tag:validation  # Combine selectors
```

Combine selectors:
```bash
dbt run -s staging,tag:daily       # Multiple selectors with OR logic
dbt run -s +model_name tag:qof     # Intersection (AND logic)
dbt run -s path:modelling/olids --exclude tag:deprecated  # With exclusions
```

### Testing and Documentation

```bash
dbt test                           # Run all tests
dbt test -s model_name             # Test specific model
dbt show -s model_name             # Preview model results (first 5 rows)
dbt docs generate                  # Generate documentation
dbt docs serve                     # Open documentation in browser
```

### Generating Model YAML

Use the codegen package to generate YAML outlines:

```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["your-model-name"], "upstream_descriptions": true}'
```

## Materializations

dbt supports different materializations that determine how models are built in the database:

- `view` - Creates a database view (default for staging)
- `table` - Creates a physical table (default for modelling/reporting)
- `incremental` - Builds incrementally, only processing new/changed records
- `ephemeral` - Creates a CTE, no object in database

Configure materialization in model config:
```sql
{{ config(materialized='table') }}
```

Or in `dbt_project.yml`:
```yaml
models:
  your_project:
    staging:
      +materialized: view
    modelling:
      +materialized: table
```

When to use each:
- **View**: Staging models, frequently changing logic, small result sets
- **Table**: Large result sets, complex transformations, reporting layers
- **Incremental**: Large datasets with frequent updates, event logs, time-series data
- **Ephemeral**: Intermediate logic reused by multiple models, not needed in database

## Ad-hoc Analysis

The `analyses/` folder contains SQL files for one-off analysis. These aren't models and won't create database objects.

Use analyses for:
- One-time data investigations
- Ad-hoc queries that reference dbt models
- Schema comparisons and row count checks
- Quick patient counts or cohort validation

Running analyses:
```bash
dbt compile -s analysis_name       # Compile the SQL
# Then execute the compiled SQL from target/compiled/
```

The compiled SQL in `target/compiled/` will have all `{{ ref() }}` and macros resolved, so you can copy and run it directly in Snowflake.

## Advanced Patterns

### Snapshots

Snapshots capture historical changes in slowly changing dimension (SCD) tables. Use snapshots when you need to track changes over time.

Creating a snapshot:
```sql
{% snapshot snapshot_name %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ ref('source_model') }}

{% endsnapshot %}
```

Running snapshots:
```bash
dbt snapshot                       # Run all snapshots
dbt snapshot -s snapshot_name      # Run specific snapshot
```

Best practices:
- Place snapshot files in `snapshots/` directory
- Use timestamp strategy for tables with `updated_at` columns
- Use check strategy for tables without timestamps (checks all columns or specified columns)
- Snapshots are always incremental - they only process new/changed records

### Incremental Models

Incremental models only process new or changed records, improving performance for large datasets.

Basic incremental model:
```sql
{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select
    id,
    created_at,
    updated_at,
    data_column
from {{ ref('source_model') }}

{% if is_incremental() %}
    -- Only process records newer than what we already have
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

Running incremental models:
```bash
dbt run                            # Run incrementally (default)
dbt run --full-refresh             # Rebuild from scratch
dbt run -s +model_name --full-refresh  # Full refresh specific model
```

Best practices:
- Use `unique_key` to handle updates (upserts)
- Always include an `is_incremental()` filter
- Use `updated_at` or `created_at` for filtering new records
- Test with `--full-refresh` occasionally to ensure logic is correct
- Consider using `on_schema_change='append_new_columns'` for evolving schemas

Common incremental strategies:
- `append` - Only insert new records (no updates)
- `merge` (default) - Insert new records and update existing (requires unique_key)
- `delete+insert` - Delete matching records then insert (requires unique_key)

## Project Architecture

### Layer-Based Structure

The project follows a medallion architecture with clear separation between layers:

```
models/
├── raw/                     # Source data with minimal transformation
│   ├── commissioning/       # Commissioning source data (views in STAGING.DBT_RAW)
│   ├── olids/               # OLIDS source data (views in STAGING.DBT_RAW)
│   ├── pid_env/             # PID environment source data (views in STAGING.DBT_RAW)
│   └── shared/              # Shared reference data (views in STAGING.DBT_RAW)
│
├── staging/                 # Cleaned and standardised source data (views in STAGING, schema per source)
│   ├── commissioning/       # Commissioning feeds (csds/ → STAGING.CSDS, sus/ → STAGING.SUS, ...)
│   ├── olids/               # OLIDS mappings (STAGING.OLIDS)
│   └── shared/              # Reference data (dictionary/ → STAGING.DICTIONARY, ...)
│
├── modelling/               # Business logic and transformations
│   ├── commissioning/       # Commissioning intermediate models (subdomain schemas)
│   └── olids/               # OLIDS intermediate models (subdomain schemas auto-generated)
│
├── reporting/               # Aggregated and analytical models
│   ├── commissioning/       # Commissioning reporting (subdomain schemas)
│   └── olids/               # OLIDS reporting (subdomain schemas auto-generated)
│
└── published/               # Production-ready datasets for end users
    ├── direct_care/         # Direct care outputs
    └── secondary_use/       # Secondary use outputs
```

### Domain Organisation

Models are organised by domain (commissioning, olids, shared) with subdomain folders for specific functional areas.

Commissioning modelling subdomains:
- `administrative/` - Administrative and organisational data
- `diagnosis/` - Diagnostic codes and disease groupings
- `encounters/` - Patient encounters and activity
- `observations/` - Clinical observations and measurements
- `procedure/` - Procedures and interventions

Commissioning reporting subdomains:
- `person_history/` - Person-level historical views
- `person_level/` - Person-level aggregations
- `provider_level/` - Provider-level aggregations

OLIDS modelling subdomains:
- `diagnoses/` - QOF disease registers and clinical diagnoses
- `geography/` - Geographic boundaries and mappings
- `medications/` - Prescribing and medication management
- `observations/` - Clinical observations and test results
- `organisation/` - Practice and organisational hierarchies
- `person_attributes/` - Patient demographics and registration
- `programme/` - Clinical programmes (immunisations, screening, etc.)
- `utilities/` - Helper models and calculations

OLIDS reporting subdomains:
- `clinical_safety/` - Clinical safety indicators
- `data_quality/` - Data quality metrics
- `definitions/` - Cohort and condition definitions
- `disease_registers/` - Disease register analytics
- `geography/` - Geographic analysis
- `measures/` - Clinical and quality measures
- `organisation/` - Practice-level reporting
- `person_analytics/` - Person-level analytics
- `person_demographics/` - Demographic reporting
- `person_status/` - Registration and status tracking
- `programme/` - Programme-level reporting

## Schema and Database Generation

Custom macros override dbt defaults to provide flexible naming:

### Database Naming

Configured via `generate_database_name` macro:

- **Production**: Base database name (e.g., `REPORTING`)
- **Development**: Prefixed with `DEV__` (e.g., `DEV__REPORTING`)

The macro automatically detects your target and applies the appropriate prefix.

### Schema Naming

Configured via `generate_schema_name` macro with two approaches:

1. Explicit schemas (default):
- Uses exact `+schema:` value from config
- No target prefix added
- Example: `COMMISSIONING_REPORTING`, `REFERENCE`

2. Automatic schemas (for configured domains):
- Schema names automatically derived from subdomain folder structure
- Pattern: `{DOMAIN}_{SUBDOMAIN}`
- Example: `models/modelling/olids/diagnoses/` → `OLIDS_DIAGNOSES`
- Configure via `vars.auto_schema_domains` in `dbt_project.yml`
- Currently enabled for: `olids`

3. Staging layer (always automatic):
- Schema = the source-system folder containing the model, upper-cased
- Example: `models/staging/commissioning/csds/` → `STAGING.CSDS`
- Example: `models/staging/olids/` → `STAGING.OLIDS`

### Adding New Folders

For domains using automatic schemas:
Simply create the subdomain folder - no config changes needed. The schema name will be automatically generated.

For other domains:
Update `dbt_project.yml` with appropriate `+database:` and `+schema:` settings:

```yaml
models:
  your_project:
    modelling:
      your_domain:
        +database: MODELLING
        +schema: YOUR_DOMAIN_MODELLING
        your_subdomain:
          +schema: YOUR_DOMAIN_YOUR_SUBDOMAIN
```


### Full Object Naming Examples

| Target | Config | Folder Path | Database | Schema | Result |
|--------|--------|-------------|----------|--------|--------|
| dev | `+database: REPORTING`<br>`+schema: COMMISSIONING_REPORTING` | `reporting/commissioning/` | `DEV__REPORTING` | `COMMISSIONING_REPORTING` | `DEV__REPORTING.COMMISSIONING_REPORTING.model` |
| prod | `+database: REPORTING`<br>`+schema: COMMISSIONING_REPORTING` | `reporting/commissioning/` | `REPORTING` | `COMMISSIONING_REPORTING` | `REPORTING.COMMISSIONING_REPORTING.model` |
| dev | Auto-schema enabled | `modelling/olids/diagnoses/` | `DEV__MODELLING` | `OLIDS_DIAGNOSES` | `DEV__MODELLING.OLIDS_DIAGNOSES.model` |
| prod | Auto-schema enabled | `modelling/olids/diagnoses/` | `MODELLING` | `OLIDS_DIAGNOSES` | `MODELLING.OLIDS_DIAGNOSES.model` |

## Roles and Permissions

### Primary Role: ANALYST

This project uses the **ANALYST** role, which has access to:
- `DATA_LAKE.*` - Source data
- `Dictionary.*` - Reference data
- `MODELLING.*` - Intermediate processing
- `REPORTING.*` - Final marts
- `PUBLISHED_REPORTING__SECONDARY_USE.*` - Published outputs
- `PUBLISHED_REPORTING__DIRECT_CARE.*` - Direct care outputs

### Role Hierarchy

```
ANALYST (base role, owns all dbt-created objects)
  ↓
ENGINEER (inherits ANALYST permissions)
  ↓
DATA_PLATFORM_MANAGER (inherits ENGINEER permissions)
```

Models can be run using any of these roles. Ownership is automatically transferred to ANALYST with `COPY CURRENT GRANTS`, meaning ANALYST becomes the owner while ENGINEER and DATA_PLATFORM_MANAGER retain management access through role inheritance.

## Custom Macros

### Model Comments

The `add_model_comment` and `generate_table_comment` macros automatically add metadata comments to all models:

```sql
-- Automatically added to all models
-- Domain: olids
-- Layer: reporting
-- Updated: 2025-10-26
```

These comments help with discoverability and understanding model purpose in Snowflake.

### Other Macros

Explore the `macros/` directory for additional utilities:
- Date and time calculations
- String manipulation
- Custom aggregations
- Testing helpers

## dbt Packages

This project uses several dbt packages (committed to `dbt_packages/` for Snowflake native execution):

- **dbt_utils** - Common utilities and macros
- **dbt_expectations** - Data quality tests
- **dbt_date** - Date manipulation helpers
- **codegen** - Code generation utilities

This project runs on the dbt Fusion engine. Generic test arguments should use the `arguments:` property.

## Working with Profiles

The `profiles.yml` file is committed for Snowflake native execution but uses `git skip-worktree` to prevent committing local credentials.

The `start_dbt.ps1` script manages this automatically. If you need to manually manage skip-worktree:

```bash
# Enable skip-worktree (ignore local changes)
git update-index --skip-worktree profiles.yml

# Disable skip-worktree (track changes again)
git update-index --no-skip-worktree profiles.yml

# Check skip-worktree status
git ls-files -v | grep "^S"
```

## Troubleshooting

### Common Issues

**Import errors when running Python scripts:**
```bash
# Ensure virtual environment is activated
venv\Scripts\activate
```

**Authentication failures:**
```bash
# Test connection
dbt debug

# Check credentials in .env
cat .env
```

**Models building to wrong schema:**
- Check `dbt_project.yml` configuration
- Verify folder structure matches expected patterns
- Check for explicit `+schema:` config in model files

**Skip-worktree not working:**
```bash
# Re-run setup script
.\start_dbt.ps1
```

### Getting Help

- Check [GitHub Issues](https://github.com/wnl-icb-analytics/dbt-analytics/issues)
- Review dbt error messages carefully
- Use `dbt compile` to see generated SQL
- Check `target/compiled/` and `target/run/` for debugging
