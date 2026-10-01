{{
    config(
        materialized='view'
    )
}}

/*
Membership-collapsed view of cltcs_cohort_data for SCD2 snapshotting.

Clinical Purpose:
- Feed `cltcs_cohort_membership_snapshot` so that we can track when patients
  enter, exit and re-enter the C-LTCS cohort, plus when their processing
  area changes. A change in `area_code` represents a change in processing
  pathway and is treated as the end of one stint and the start of another.

Shape:
- One row per patient_id (carrying current area_code).
- The wide attribute set from cltcs_cohort_data is collapsed into a single
  VARIANT column `attributes_json` so the snapshot stays narrow while still
  preserving the patient's state at the start of each stint.
- `OBJECT_CONSTRUCT_KEEP_NULL` is used so null-valued fields are retained
  in the JSON (distinguishing "null at entry" from "field absent").

Snapshot (see docs/archive/snapshots-guide.md):
- `strategy: check` with selective `check_cols` on `area_code` only; the
  patient is identified by `unique_key: patient_id`. Inclusion and removal
  are tracked when the row appears or disappears from this view
  (`hard_deletes: invalidate`). `attributes_json` is not in `check_cols`, so
  it is captured when a new stint opens and does not reopen a stint when
  only attributes drift.
- `table_refresh_date` is the project convention for the optional `updated_at`
  column used alongside the check strategy.

Downstream:
- `snapshots/cltcs_cohort_membership_snapshot.yml`
*/

select
      patient_id
    , area_code
    , re_id_key
    , object_construct_keep_null(
          * exclude (patient_id, area_code, re_id_key)
      ) as attributes_json
    , current_timestamp()::timestamp_ntz as table_refresh_date
from {{ ref('cltcs_cohort_data') }}
