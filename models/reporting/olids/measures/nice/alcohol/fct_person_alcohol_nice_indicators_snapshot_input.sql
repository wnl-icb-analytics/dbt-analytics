{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_alcohol_nice_indicators for SCD2 snapshotting. Drops
reporting_date and measurement_period_start, which advance on every build. A version opens on a new screen or intervention, a practice move, or a record ageing out of the window.

Downstream: snapshots/fct_person_alcohol_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , latest_alcohol_screen_date
    , latest_positive_alcohol_screen_date
    , latest_intervention_after_positive_screen_date
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_alcohol_nice_indicators') }}
