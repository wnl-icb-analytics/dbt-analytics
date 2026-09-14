{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_shingles_vaccination_ind219 for SCD2 snapshotting. Drops
reporting_date and measurement_period_start, which advance on every build. A version opens on a vaccination record or a practice move; people leave when their 75th birthday passes out of the cohort window.

Downstream: snapshots/fct_person_shingles_vaccination_ind219_snapshot.yml
*/

select
    person_id
    , current_practice_code
    , first_dose_70_to_75_date
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_shingles_vaccination_ind219') }}
