# Working with Sources

A quick reference for adding, updating, and regenerating sources in this project.
See [Project conventions](../PROJECT_CONVENTIONS.md) for the raw and staging
contracts, and [How it works](#how-it-works) further down for pipeline background.

## The raw layer

Every source table gets a generated **raw model** in `models/raw/<domain>/`. Raw models are a 1:1 passthrough — one `SELECT` that reads the source and renames its columns to snake_case. They exist so the rest of the project can `ref()` a stable name instead of calling `source()` with a quoted identifier and reaching directly into Snowflake.

A generated raw model looks like this:

```sql
-- models/raw/shared/raw_reference_imd_2025.sql (generated)
{{ config(description="Raw layer (Analyst-managed reference datasets...)...") }}
select
    "LSOA_CODE_2021" as lsoa_code_2021,
    "LSOA_NAME_2021" as lsoa_name_2021,
    "INDEX_OF_MULTIPLE_DEPRIVATION_IMD_SCORE" as index_of_multiple_deprivation_imd_score,
    -- ...
from {{ source('reference_analyst_managed', 'IMD_2025') }}
```

Rules:

- **File name**: `{raw_prefix}_{table_name_sanitised}.sql`. `raw_prefix` comes from `source_mappings.yml`.
- **Folder**: `models/raw/<domain>/`, where `domain` also comes from `source_mappings.yml` (`commissioning`, `olids`, `shared`, `pid_env`).
- **Column aliases**: originals are double-quoted (preserves case), aliases are snake_case. Special characters get cleaned (`%` → `percent`, `#` → `number`, `&` → `and`, spaces/dashes/dots → `_`). CamelCase becomes snake_case with acronym handling (`GPPracticeCode` → `gp_practice_code`). Reserved words like `group`, `order`, `where` get `_value` appended. Columns starting with a digit get a `col_` prefix or are reordered.
- **Never edit by hand** — raw models are regenerated on every pipeline run.

New staging models and changed dependencies should reference raw models via
`ref()`, never the source directly:

```sql
-- Correct (in a staging model)
select
    lsoa_code_2021,
    lsoa_name_2021
from {{ ref('raw_reference_imd_2025') }}

-- Avoid - bypasses the raw layer and its cleaned column names
select
    "LSOA_CODE_2021",
    "LSOA_NAME_2021"
from {{ source('reference_analyst_managed', 'IMD_2025') }}
```

This gives the project one place where source column names are mapped, quoted
identifiers are handled, and the interface between dbt and Snowflake is defined.
Only staging models may reference `raw_` models; modelling, reporting and product
models must start from staging or a later contract.

A small number of existing staging models still call `source()` directly. Treat
these as legacy debt, not examples to copy. When changing one of those models,
replace the call with `ref()` to its generated raw model as part of the change.
Do not expand the pull request into unrelated staging models.

## Quick reference


| I want to...                                                    | Do this                                                                                           |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Regenerate all sources and raw models                           | `python scripts/sources/run_all_source_generation.py`                                             |
| Regenerate just the YAMLs and raw models (no Snowflake refresh) | `python scripts/sources/2_generate_sources.py && python scripts/sources/3_generate_raw_models.py` |
| Add a new database/schema with all its tables                   | Add to `source_mappings.yml`, run the pipeline                                                    |
| Add a single ad-hoc table from an existing schema               | Add it to a manual YAML (see below)                                                               |
| Pin a volatile schema to a curated table list                   | Use `manual: true` + a `manual_<name>.yml` file                                                   |


## Review regenerated models

After regeneration, check the raw model change warning at the end of the output.
It compares the previous files with the generated files and lists renamed,
removed or moved models, their source identities and affected literal `ref()`
calls in staging SQL and YAML. A folder move with the same model name preserves
the `ref()` name, but may change the model's configuration.
If that name is reused for a different source, the warning lists both source
identities and the staging refs to review instead of calling it a move.

A rename is identified only when the `source()` name and table name are unchanged.
A changed source or table name is reported as a removal, not a guessed replacement.
The warning does not stop generation or change staging files. Update broken refs
or restore the source routing, then run `dbt parse`. Check indirect refs and
external consumers separately. A later run compares against the files from the
previous run, so review the warning before running generation again.

The existing domain defaults and filename rules remain unchanged. For a manual
source, use `config.meta.domain` and `config.meta.raw_prefix` to set its routing.

## Adding a new source

### Option A: all tables from a stable schema

Most common. Add an entry to `scripts/sources/source_mappings.yml`:

```yaml
- source_name: my_source
  database: DATA_LAKE
  schema: MY_SCHEMA
  description: What the data contains
  raw_prefix: raw_my_source
  domain: commissioning   # commissioning | olids | shared | pid_env
```

Run `python scripts/sources/run_all_source_generation.py`. Commit the new `auto_*.yml` and `models/raw/**/*.sql` files.

Within a source entry, use `exclude_tables` when a schema object is visible in metadata but must not be generated:

```yaml
- source_name: my_source
  database: DATA_LAKE
  schema: MY_SCHEMA
  exclude_tables:
    - BROKEN_OR_UNUSED_VIEW
```

### Option B: curated table list from a volatile schema

Use this when a schema gets lots of ad-hoc uploads but the project only depends on a few tables (e.g. `ANALYST_MANAGED`).

1. Create `models/sources/manual_<name>.yml` listing only the tables you care about:
  ```yaml
   version: 2
   sources:
     - name: my_source
       database: '"DATA_LAKE__NCL"'
       schema: '"MY_SCHEMA"'
       description: Curated subset of MY_SCHEMA
       tables:
         - name: TABLE_A
           identifier: '"TABLE_A"'
           columns:
             - name: COL_1
               data_type: TEXT
  ```
2. Add a matching entry in `source_mappings.yml` with `manual: true`:
  ```yaml
   - source_name: my_source
     database: DATA_LAKE__NCL
     schema: MY_SCHEMA
     description: Curated subset of MY_SCHEMA
     raw_prefix: raw_my_source
     domain: shared
     manual: true
  ```
3. Run the pipeline. Step 2 skips auto-generating a YAML; step 3 uses the manual YAML to write raw models with the `raw_prefix` from the mapping.

Tables that appear in `MY_SCHEMA` but not in the manual YAML are ignored. Downstream refs for a removed table break explicitly.

## Drift warnings

Step 2 of the pipeline compares manual YAML files against the live Snowflake schema and prints warnings when they disagree:

```
Manual source drift check - 2 warning(s) across 14 table(s):
  [drift] manual_analyst_managed.yml: reference_analyst_managed.IMD_2025 declares columns not in source: ['NEW_SCORE_COLUMN']
  [drift] manual_analyst_managed.yml: reference_analyst_managed.PRACTICE_NEIGHBOURHOOD_LOOKUP source has undeclared columns: ['NEW_PCN_FIELD']
```


| Warning                          | Likely cause                       | What to do                                                                              |
| -------------------------------- | ---------------------------------- | --------------------------------------------------------------------------------------- |
| `declares columns not in source` | Column renamed or dropped upstream | Update the manual YAML and any downstream models that reference the old name            |
| `source has undeclared columns`  | New column added upstream          | Add it to the manual YAML if you want to use it; otherwise ignore                       |
| `not found in live metadata`     | Table renamed or dropped upstream  | Remove the entry from the manual YAML and fix downstream refs, or point at the new name |


`data_type` changes (e.g. `NUMBER(38,0)` → `NUMBER(38,2)`) are synced silently into the manual YAML on every run — you don't need to edit them by hand.

## Common pitfalls

- **Never edit `models/raw/**/*.sql` or `auto_*.yml` by hand** — both are regenerated from the source YAMLs on every run. Fix the source YAML instead.
- **`DEV__` vs prod database** — `sources.yml` entries often use `DEV__<database>` so dev builds work. When prod and dev schemas drift, raw models can compile against a table shape that only exists in one environment. If a raw model throws `invalid identifier`, check that `source_mappings.yml` and the manual YAML both point at the same database as the raw model is being built against.
- **Duplicate source names** — step 3 fails fast if the same source name appears in more than one manual YAML file. Don't copy-paste source blocks between files.
- **Missing mapping** — if step 3 warns `No mapping found for source '<name>'`, add the source to `source_mappings.yml` (with `manual: true` if it's a manual YAML) so raw models pick up the right `raw_prefix` and `domain`.

---

## How it works

This section is for context — day-to-day work doesn't need it.

### Pipeline scripts

```
scripts/sources/
  source_mappings.yml            # Source registry (every source in the project)
  1a_generate_metadata_query.py  # Writes metadata_query.sql from the mappings
  1b_extract_metadata.py         # Runs the query via Snowflake SSO -> table_metadata.csv
  2_generate_sources.py          # Writes auto_*.yml + drift check on manual YAMLs
  3_generate_raw_models.py       # Writes models/raw/**/*.sql from all source YAMLs
  run_all_source_generation.py   # Runs 1a -> 1b -> 2 -> 3
```

Step `1b` opens a browser for Snowflake SSO. Steps `2` and `3` are offline.

### Source file types


| File pattern                       | Editable? | Where the tables come from                     |
| ---------------------------------- | --------- | ---------------------------------------------- |
| `models/sources/auto_*.yml`        | No        | Snowflake `INFORMATION_SCHEMA` at step 2       |
| `models/sources/sources.yml`       | Yes       | Hand-written (shared file for several sources) |
| `models/sources/manual_<name>.yml` | Yes       | Hand-written (dedicated file per source)       |


Manual sources override auto-generated sources with the same name. Step 2 removes the source from `auto_*.yml` automatically if it gets added to a manual file.

### The `manual: true` flag

In `source_mappings.yml`, entries tagged `manual: true` still appear in the metadata query (so drift detection can compare against the live schema), but step 2 skips writing an `auto_*.yml`. The mapping is kept only for its `raw_prefix` and `domain`, which step 3 uses when generating raw models from the manual YAML.

### Drift checks in step 2

Two things happen to every manual source whose `(database, schema)` matches a mapping in `source_mappings.yml`:

1. **Silent type sync** — `data_type` values in the manual YAML are rewritten to match live metadata. No warning is printed for type changes because they are idempotent and don't break downstream SQL.
2. **Warning-only drift check** — added, removed, or renamed columns and missing tables are reported but not auto-corrected. These need human review.

### Pipeline failures


| Failure                                                            | Cause                                         | Fix                                                                                                                                           |
| ------------------------------------------------------------------ | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `SQL file not found` in `1b`                                       | Step `1a` wasn't run                          | Run `1a` first                                                                                                                                |
| `Error during metadata extraction` in `1b`                         | Snowflake auth or query error                 | The stale `table_metadata.csv` is removed automatically so step `2` can't silently consume it. Re-run `1b` once the underlying error is fixed |
| `source '<name>' is declared in both '<file>' and '<file>'` in `3` | Two manual YAMLs declare the same source name | Remove the duplicate                                                                                                                          |


