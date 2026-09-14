{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_antithrombotic_therapy_nice_indicators
for SCD2 snapshotting at person and indicator grain. Drops reporting_date and
measurement_period_start, which advance on every build. A version opens on a new
order, a practice move or an order ageing out of the window.

Downstream: snapshots/fct_person_antithrombotic_therapy_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , latest_antiplatelet_order_date
    , latest_anticoagulant_order_date
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_antithrombotic_therapy_nice_indicators') }}
