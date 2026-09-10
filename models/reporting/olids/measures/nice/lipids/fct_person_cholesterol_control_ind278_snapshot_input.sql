{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_cholesterol_control_ind278 for SCD2 snapshotting.

The measure carries reporting_date and measurement_period_start, both derived from
CURRENT_DATE(), so snapshotting the full model with check_cols: all would version every
person on every build. This view keeps the qualifying registers, current practice, the
selected lipid result and the indicator outcome. A version opens on a genuine change: a new
result, a practice move, a register change, or the selected result ageing out of the
12-month window.

Downstream: snapshots/fct_person_cholesterol_control_ind278_snapshot.yml
*/

select
    person_id
    , current_practice_code
    , has_chd
    , has_stroke_tia
    , has_pad
    , latest_lipid_observation_id
    , latest_lipid_date
    , lipid_type
    , latest_lipid_value
    , unit_status
    , plausibility_status
    , is_latest_lipid_review_required
    , is_lipid_recorded_in_last_12m
    , is_latest_lipid_valid
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_cholesterol_control_ind278') }}
