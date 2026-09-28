{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_atrial_fibrillation_nice_indicators
for SCD2 snapshotting at person and indicator grain. Drops reporting_date and
measurement_period_start, which advance on every build. A version opens on a new
score, order or review, a practice move, or a record ageing out of the window.

Downstream: snapshots/fct_person_atrial_fibrillation_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , latest_chadsvasc_score
    , latest_anticoagulant_order_date
    , latest_anticoagulant_type
    , latest_anticoagulant_review_date
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_atrial_fibrillation_nice_indicators') }}
