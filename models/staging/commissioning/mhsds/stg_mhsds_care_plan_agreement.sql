{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs009careplanagreement')) }}
)
select
    mhs009_uniq_id
    , person_id
    , uniq_care_plan_id
    , family_care_plan_indicator
    , no_family_care_plan_reason
    , care_plan_content_agreed_by
    , care_plan_agreed_by
    , iff(care_plan_content_agreed_date::date >= '1901-01-01'::date, care_plan_content_agreed_date::date, null) as care_plan_content_agreed_date
    , iff(care_plan_agreed_date::date >= '1901-01-01'::date, care_plan_agreed_date::date, null) as care_plan_agreed_date
    , care_plan_content_agreed_time::time as care_plan_content_agreed_time
    , care_plan_agreed_time::time as care_plan_agreed_time
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
