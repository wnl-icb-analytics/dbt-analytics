{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_bmi_recording_ind320 for SCD2 snapshotting. Drops
reporting_date and measurement_period_start, which advance on every build. A version opens on a new BMI, a practice move, or a BMI ageing out of the window.

Downstream: snapshots/fct_person_bmi_recording_ind320_snapshot.yml
*/

select
    person_id
    , current_practice_code
    , latest_record_date
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_bmi_recording_ind320') }}
