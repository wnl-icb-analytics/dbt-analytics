{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_cervical_screening_nice_indicators for SCD2 snapshotting. Drops
reporting_date and measurement_period_start, which advance on every build. A version opens on a new screen, a practice move, or a screen ageing out of the window.

Downstream: snapshots/fct_person_cervical_screening_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , latest_completed_date
    , latest_screening_date
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_cervical_screening_nice_indicators') }}
