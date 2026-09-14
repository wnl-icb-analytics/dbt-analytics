{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_childhood_immunisation_nice_indicators for SCD2 snapshotting. Drops
reporting_date and measurement_period_start, which advance on every build. A version opens on a new dose or a practice move; children leave when their milestone passes out of the cohort window.

Downstream: snapshots/fct_person_childhood_immunisation_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , milestone_date
    , doses_in_window
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_childhood_immunisation_nice_indicators') }}
