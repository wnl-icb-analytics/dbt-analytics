{{ config(materialized='view') }}

/*
Thin snapshot-input projection of fct_person_flu_vaccination_nice_indicators for SCD2 snapshotting. Drops
reporting_date and measurement_period_start, which advance on every build. A version opens on a vaccination, a practice move, or the season rolling over.

Downstream: snapshots/fct_person_flu_vaccination_nice_indicators_snapshot.yml
*/

select
    person_id
    , indicator_id
    , current_practice_code
    , campaign_id
    , latest_vaccination_date
    , is_in_numerator
    , indicator_status
from {{ ref('fct_person_flu_vaccination_nice_indicators') }}
