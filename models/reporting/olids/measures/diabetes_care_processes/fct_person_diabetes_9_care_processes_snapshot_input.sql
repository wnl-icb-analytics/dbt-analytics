{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_diabetes_9_care_processes for SCD2 snapshotting.

The care-processes model is a rolling-12-month metric: care_processes_completed and every
*_completed_in_last_12m flag are computed against CURRENT_DATE(), so they change with the
clock (a reading ageing past 12 months) rather than only when a new reading arrives.
Snapshotting those columns would either version every person on every build (check_cols: all)
or, if excluded from check_cols, freeze stale ride-along values. So this view exposes only the
STABLE per-reading inputs -- the latest measurement dates, the latest HbA1c value, and a
date-independent foot_check_qualifies flag -- which change only on a genuine new reading. The
rolling 12-month window is re-applied downstream anchored on each index_date (in
cltcs_monthly_covariates), reproducing the care_processes_completed count and the HbA1c
recency gate as-at that date.

latest_retinal_screening_date rides along so the 9th care process is available later; the
current covariate contract uses the 8-process count.

foot_check_qualifies mirrors the non-date part of fct_person_diabetes_8_care_processes'
foot_check_completed_in_last_12m: an adequate examination of both feet (allowing for an
absent/amputated foot) that was not declined or deemed unsuitable. It is read from the same
latest foot-examination record that supplies latest_foot_check_date, so the two stay in step.

Downstream: snapshots/fct_person_diabetes_9_care_processes_snapshot.yml
*/

select
    d.person_id
    , d.latest_hba1c_date
    , d.latest_hba1c_value
    , d.latest_bp_date
    , d.latest_cholesterol_date
    , d.latest_creatinine_date
    , d.latest_acr_date
    , d.latest_foot_check_date
    , d.latest_bmi_date
    , d.latest_smoking_date
    , d.latest_retinal_screening_date
    , coalesce(
          (
              fc.both_feet_checked
              or (fc.left_foot_checked and (fc.right_foot_absent or fc.right_foot_amputated))
              or (fc.right_foot_checked and (fc.left_foot_absent or fc.left_foot_amputated))
          )
          and not (fc.is_unsuitable or fc.is_declined)
      , false) as foot_check_qualifies
from {{ ref('fct_person_diabetes_9_care_processes') }} d
left join {{ ref('int_foot_examination_latest') }} fc
    on d.person_id = fc.person_id
