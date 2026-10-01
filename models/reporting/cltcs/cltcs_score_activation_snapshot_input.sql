{{
    config(
        materialized='view'
    )
}}

/*
Narrow snapshot-input view of cltcs_score_activation for SCD2 snapshotting.

Clinical Purpose:
- Feed `cltcs_score_activation_snapshot` so the activation sub-score can be
  tracked over time. This point-in-time history is used to match patients
  against a control cohort in future (comparing like-for-like score state
  as-at a given date).

Shape:
- One row per patient (`sk_patient_id`).
- Only the identifier and the final activation score are carried. Unlike the
  treatment/frailty scores, cltcs_score_activation exposes no scaled subdomain
  components -- it is a direct arithmetic score -- so there is nothing further
  to carry.

Snapshot (see docs/archive/snapshots-guide.md):
- `strategy: check` keyed on `unique_key: sk_patient_id`.
- `check_cols` is `score_activation` only.
  NOTE: unlike the cohort-relative, 1dp-rounded treatment/frailty scores,
  score_activation is a raw (unrounded) formula over rolling-12-month activity
  (A&E / GP counts), so it moves whenever those rolling windows shift -- expect
  more frequent versions than the treatment/frailty snapshots.
- `hard_deletes: invalidate` closes a patient's open row when they leave the
  scored population.
- `table_refresh_date` is the project convention for the optional `updated_at`
  column used alongside the check strategy.

Downstream:
- `snapshots/cltcs_score_activation_snapshot.yml`
*/

select
      sk_patient_id
    -- final activation score
    , score_activation
    , current_timestamp()::timestamp_ntz as table_refresh_date
from {{ ref('cltcs_score_activation') }}
