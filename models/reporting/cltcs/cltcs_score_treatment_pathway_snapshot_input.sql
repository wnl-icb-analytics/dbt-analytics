{{
    config(
        materialized='view'
    )
}}

/*
Narrow snapshot-input view of cltcs_score_treatment_pathway for SCD2 snapshotting.

Clinical Purpose:
- Feed `cltcs_score_treatment_pathway_snapshot` so the candidate treatment score,
  its scaled domain components and priority tier can be tracked over time beside
  the current treatment score, for control-cohort matching and for comparing the
  two scores as-at a given date.

Shape:
- One row per patient (`sk_patient_id`) per inclusion criteria.
- Only the identifier, the scaled domain scores, the composite z-score, the
  unadjusted and age-adjusted final scores and the priority tier are carried, so
  the snapshot stays narrow rather than versioning the full wide score model
  (raw/transformed/centred/bounded/0-100 intermediates).

Snapshot (see docs/archive/snapshots-guide.md):
- `strategy: check` keyed on `unique_key: [sk_patient_id, inclusion_rule]`.
- `check_cols` is `score_treatment` and `score_treatment_age_adjusted`. Both are
  checked because the age-adjusted score is capped at 100 (so it can stay fixed
  while the unadjusted score moves) and changes with age alone. The scaled domain
  scores, composite z-score and tier are carried but do not themselves open a
  version; they are captured as-at each version open.
  NOTE: these scores are cohort-relative (centred per neighbourhood and scaled by
  the cohort's pooled standard deviation), so the scores can drift between builds
  without an underlying feature change; both are rounded to whole points as the
  model's final step, so drift of under half a point does not version, which limits
  churn.
- `hard_deletes: invalidate` closes a patient's open row when they leave the
  scored population.
- `table_refresh_date` is the project convention for the optional
  `updated_at` column used alongside the check strategy.

Downstream:
- `snapshots/cltcs_score_treatment_pathway_snapshot.yml`
*/

select
      sk_patient_id
    , inclusion_rule
    -- scaled domain scores (cohort-relative z-scores, before the soft clip)
    , scaled_score_disease_control
    , scaled_score_monitoring_gaps
    , scaled_score_treatment_optimisation
    , scaled_score_complexity
    , scaled_score_illness_uec
    , scaled_score_barriers
    , scaled_score_ltc_lcs_engagement
    -- re-standardised composite
    , composite_z
    -- final scores and priority tier
    , score_treatment
    , score_treatment_age_adjusted
    , treatment_priority_tier
    , current_timestamp()::timestamp_ntz as table_refresh_date
from {{ ref('cltcs_score_treatment_pathway') }}
