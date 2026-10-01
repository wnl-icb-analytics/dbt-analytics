{{ config(materialized='view') }}

/*
Thin snapshot-input projection of dim_person_demographics for SCD2 snapshotting.

Exposes only stable / structural demographic match keys (for control-cohort
matching and to track structural change like practice moves or death).
`age` is deliberately EXCLUDED: it changes continuously and would version every
person on their birthday. Track structure, not age.

Downstream: snapshots/dim_person_demographics_snapshot.yml
*/

select
    person_id
    , gender
    , ethnicity_category
    , ethnicity_subcategory
    , ethnicity_granular
    , main_language
    , language_type
    , practice_code
    , pcn_code
    , neighbourhood_registered
    , is_deceased
    , death_date_approx
from {{ ref('dim_person_demographics') }}
