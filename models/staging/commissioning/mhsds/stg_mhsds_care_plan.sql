{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs008careplantype')) }}
)
select
    mhs008_uniq_id
    , person_id
    , uniq_care_plan_id
    , care_plan_type_mh
    , iff(care_plan_creat_date::date >= '1901-01-01'::date, care_plan_creat_date::date, null) as care_plan_creat_date
    , care_plan_creation_time::time as care_plan_creation_time
    , iff(care_plan_last_update_date::date >= '1901-01-01'::date, care_plan_last_update_date::date, null) as care_plan_last_update_date
    -- Source time fields use a 1970 date anchor; retain their time of day.
    , care_plan_last_update_time::time as care_plan_last_update_time
    , iff(care_plan_implement_date::date >= '1901-01-01'::date, care_plan_implement_date::date, null) as care_plan_implement_date
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
