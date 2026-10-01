{{
    config(
        materialized='view'
    )
}}

/*
Narrow snapshot-input view of cltcs_score_frailty for SCD2 snapshotting.

Clinical Purpose:
- Feed `cltcs_score_frailty_snapshot` so the frailty sub-score and its scaled
  subdomain components can be tracked over time. This point-in-time history is
  used to match patients against a control cohort in future (comparing
  like-for-like score state as-at a given date).

Shape:
- One row per patient (`sk_patient_id`).
- Only the identifier, the scaled subdomain scores, the care-home exclusion
  component and the final score are carried, so the snapshot stays narrow
  rather than versioning the full wide score model.
- `score_exclusions` is included because it materially shifts the final
  `score_frailty` (a 50-point penalty per care-home exclusion) and is worth
  preserving alongside the score for matching.

Snapshot (see docs/archive/snapshots-guide.md):
- `strategy: check` keyed on `unique_key: sk_patient_id`.
- `check_cols` is `score_frailty` only. The scaled subdomain scores and
  `score_exclusions` are carried but do not themselves open a version; they
  are captured as-at each version open. Because the final score is derived
  from the scaled scores (and the care-home exclusion penalty), any material
  change moves `score_frailty` (rounded to 1dp) and opens a version, which
  captures the current components.
  NOTE: these scores are cohort-relative (z-scored per neighbourhood), so
  `score_frailty` can still drift between builds without an underlying
  feature change; sub-0.1 drift that does not move the rounded score will not
  version, which limits churn.
- `hard_deletes: invalidate` closes a patient's open row when they leave the
  scored population.
- `table_refresh_date` is the project convention for the optional
  `updated_at` column used alongside the check strategy.

Downstream:
- `snapshots/cltcs_score_frailty_snapshot.yml`
*/

select
      sk_patient_id
    -- scaled subdomain scores (cohort-relative z-scores)
    , scaled_score_clinical_complexity
    , scaled_score_clinical_frailty
    , scaled_score_medicines_management
    , scaled_score_emergency_use
    , null as scaled_score_residential_social_factors
    , scaled_score_wider_care_engagement
    , scaled_score_asc_indicators
    -- care-home exclusion component (drives the final-score penalty)
    , score_exclusions
    -- final frailty score
    , score_frailty
    , current_timestamp()::timestamp_ntz as table_refresh_date
from {{ ref('cltcs_score_frailty') }}
