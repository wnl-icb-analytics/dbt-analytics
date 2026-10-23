# Acute reporting table materialisation

## Decision record

- **Date:** 12 October 2026
- **Requested by:** Ibrahim Somaiya (Ibby)
- **Area:** `REPORTING.ACUTE`
- **Decision:** Materialise every acute reporting model as a table and rebuild the models daily.

## Request and rationale

Ibby asked whether the acute reporting outputs should all be tables because they contain joins, or are expected to gain joins and derived logic as the reporting layer develops. The team agreed to standardise the folder rather than retain a mixture of tables and views.

Materialising the models as tables means joins and calculations run once during the dbt build instead of being repeated for every analyst query. This provides consistent object behaviour in `REPORTING.ACUTE` and more predictable query performance for users.

## Change made

The following models were already tables and remain unchanged:

- `fct_person_sus_apc_recent`
- `fct_person_sus_uec_recent`
- `obt_encounter_uec`

The following models are changed from views to tables:

- `fct_person_sus_op_recent`
- `obt_appointment_op`
- `obt_encounter_apc`
- `obt_encounter_op`

The `models/reporting/acute` folder is tagged `daily` in `dbt_project.yml`. The scheduled daily selector uses `+tag:daily`, so dbt selects the acute models and their upstream dependencies. The existing workflow runs the daily job at 04:00 UTC from Tuesday to Sunday; the Monday weekly build also includes these models. This gives the acute tables one scheduled rebuild each day.

## Operational effect

- Analysts query persisted tables instead of executing the underlying joins on every query.
- Data reflects the most recent successful scheduled build rather than changes in upstream data appearing immediately through a view.
- A failed build preserves the previously built table; the dbt job and tests provide the refresh status.
- The change does not alter model SQL, column names, or business logic.

## Review focus

Reviewers should confirm that a daily refresh is sufficiently frequent for acute reporting and that downstream consumers do not rely on immediate pass-through changes from the former views.
