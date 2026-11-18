{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_childhood_immunisation_nice_indicators for SCD2 snapshotting. Drops
reporting_date and measurement_period_start, which advance on every build.
Changes to dose counts, MenB dose roles or practice open a new version.
Children leave when their milestone passes out of the cohort window.

Downstream: snapshots/fct_person_childhood_immunisation_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , milestone_date
    , doses_in_window
    , primary_doses_by_12_months
    , booster_doses_12_to_18_months
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_childhood_immunisation_nice_indicators') }}
