{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_bp_control for SCD2 snapshotting.

fct_person_bp_control carries CURRENT_DATE()-derived columns (latest_bp_reading_age_months,
is_latest_bp_within_recommended_interval) that change on every run, so snapshotting the full
model with check_cols: all would version every person on every build. This view exposes only
the stable BP-control covariates needed downstream -- the overall NG136 control flag and the
latest reading -- so the snapshot tracks genuine changes (a new reading, or a control-status
change when the patient-specific threshold moves).

Downstream: snapshots/fct_person_bp_control_snapshot.yml
*/

select
    person_id
    , is_overall_bp_controlled
    , latest_systolic_value
    , latest_diastolic_value
    , latest_bp_date
from {{ ref('fct_person_bp_control') }}
