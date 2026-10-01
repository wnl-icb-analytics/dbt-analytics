{{
    config(
        materialized='view'
    )
}}

/*
Narrow snapshot-input view of cltcs_score_treatment for SCD2 snapshotting.

Clinical Purpose:
- Feed `cltcs_score_treatment_snapshot` so the treatment sub-score and its
  scaled subdomain components can be tracked over time. This point-in-time
  history is used to match patients against a control cohort in future
  (comparing like-for-like score state as-at a given date).

Shape:
- One row per patient (`sk_patient_id`).
- Only the identifier, the scaled subdomain scores and the final
  age-adjusted score are carried, so the snapshot stays narrow rather than
  versioning the full wide score model (raw/clipped/0-100 intermediates).

Snapshot (see docs/archive/snapshots-guide.md):
- `strategy: check` keyed on `unique_key: sk_patient_id`.
- `check_cols` is `score_treatment` only. The scaled subdomain scores are
  carried but do not themselves open a version; they are captured as-at each
  version open. Because the final score is derived from the scaled scores,
  any material change in the components moves `score_treatment` (rounded to
  1dp) and opens a version, which captures the current scaled scores.
  NOTE: these scores are cohort-relative (z-scored per neighbourhood, then age
  adjusted), so `score_treatment` can still drift between builds without an
  underlying feature change; sub-0.1 drift that does not move the rounded
  score will not version, which limits churn.
- `hard_deletes: invalidate` closes a patient's open row when they leave the
  scored population.
- `table_refresh_date` is the project convention for the optional
  `updated_at` column used alongside the check strategy.

Downstream:
- `snapshots/cltcs_score_treatment_snapshot.yml`
*/

select
      sk_patient_id
    -- scaled subdomain scores (cohort-relative z-scores)
    , scaled_score_biomarker_gaps
    , scaled_score_care_gaps
    , scaled_score_complexity
    , scaled_score_medication
    , scaled_score_illness_uec
    , scaled_score_barriers
    -- final age-adjusted treatment score
    , score_treatment
    , current_timestamp()::timestamp_ntz as table_refresh_date
from {{ ref('cltcs_score_treatment') }}
