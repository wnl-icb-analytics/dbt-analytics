{{
    config(
        materialized='view'
    )
}}

/*
Narrow snapshot-input view of the LTC LCS Model of Care (MoC) fields for SCD2 snapshotting.

Purpose:
- Feed `cltcs_ltc_lcs_moc_snapshot` so each person's MoC stage and pathway status can be
  read as-at a past date by the C-LTCS matched-control covariate panel
  (cltcs_monthly_covariates).
- fct_person_ltc_lcs_risk_summary_snapshot (owned by the LTC LCS programme) versions only on
  risk-group changes, so MoC progress between those changes is not recorded there. This
  C-LTCS-scoped snapshot tracks the MoC fields without changing the programme's snapshot.

Shape:
- One row per person on the LTC LCS MoC base population (`person_id`), the same population
  as fct_person_ltc_lcs_risk_summary.
- Carries the two MoC fields used for matching plus table_refresh_date.

Snapshot (see docs/archive/snapshots-guide.md):
- `strategy: check` keyed on `unique_key: person_id`, `check_cols` = the two MoC fields.
- Both fields look back over the last 12 months from the build date, so a version can open
  when an activity ages out of that window as well as when a new one is recorded.
- `hard_deletes: invalidate` closes a person's open row when they leave the MoC base
  population, so presence of a version as-at a date means "on the MoC base then".
- `table_refresh_date` is the project convention for the optional `updated_at` column used
  alongside the check strategy.

Downstream:
- `snapshots/cltcs_ltc_lcs_moc_snapshot.yml`
*/

select
      person_id
    , moc_stage_completed_label
    , moc_pathway_status
    , current_timestamp()::timestamp_ntz as table_refresh_date
from {{ ref('fct_person_ltc_lcs_risk_summary') }}
