{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_diabetes_nice_indicators for SCD2
snapshotting at person and indicator grain. Drops reporting_date and
measurement_period_start, which advance on every build. A version opens on a new
record, a practice move or a record ageing out of the window.

Downstream: snapshots/fct_person_diabetes_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , latest_record_date
    , latest_hba1c_value
    , latest_systolic_value
    , latest_diastolic_value
    , latest_frailty_severity
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_diabetes_nice_indicators') }}
