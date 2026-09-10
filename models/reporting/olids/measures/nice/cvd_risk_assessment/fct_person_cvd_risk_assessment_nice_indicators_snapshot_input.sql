{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_cvd_risk_assessment_nice_indicators
for SCD2 snapshotting at person and indicator grain. Drops reporting_date and
measurement_period_start, which advance on every build. A version opens on a new
assessment, a practice move or an assessment ageing out of the window.

Downstream: snapshots/fct_person_cvd_risk_assessment_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , latest_risk_score
    , latest_risk_assessment_date
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_cvd_risk_assessment_nice_indicators') }}
