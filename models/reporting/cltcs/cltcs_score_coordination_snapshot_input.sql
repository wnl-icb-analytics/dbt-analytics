{{
    config(
        materialized='view'
    )
}}

/*
Narrow snapshot-input view of cltcs_score_coordination for SCD2 snapshotting.

Clinical Purpose:
- Feed `cltcs_score_coordination_snapshot` so the coordination sub-score can be
  tracked over time. This point-in-time history is used to match patients
  against a control cohort in future (comparing like-for-like score state
  as-at a given date).

Shape:
- One row per patient (`sk_patient_id`).
- Only the identifier and the final coordination score are carried. Unlike the
  treatment/frailty scores, cltcs_score_coordination exposes no scaled subdomain
  components -- it is a direct arithmetic score -- so there is nothing further
  to carry.

Snapshot (see docs/archive/snapshots-guide.md):
- `strategy: check` keyed on `unique_key: sk_patient_id`.
- `check_cols` is `score_coordination` only.
  NOTE: unlike the cohort-relative, 1dp-rounded treatment/frailty scores,
  score_coordination is a raw (unrounded) formula over rolling-12-month activity
  (waiting lists, OP / GP / A&E / APC counts), so it moves whenever those rolling
  windows shift -- expect more frequent versions than the treatment/frailty
  snapshots.
- `hard_deletes: invalidate` closes a patient's open row when they leave the
  scored population.
- `table_refresh_date` is the project convention for the optional `updated_at`
  column used alongside the check strategy.

Downstream:
- `snapshots/cltcs_score_coordination_snapshot.yml`
*/

select
      sk_patient_id
    -- final coordination score
    , score_coordination
    , current_timestamp()::timestamp_ntz as table_refresh_date
from {{ ref('cltcs_score_coordination') }}
