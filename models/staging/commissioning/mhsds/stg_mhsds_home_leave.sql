{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs509homeleave')) }}
)
select
    mhs509_uniq_id
    , person_id
    , uniq_serv_req_id
    , uniq_ward_stay_id
    , uniq_hosp_prov_spell_id
    , iff(start_date_home_leave::date >= '1901-01-01'::date, start_date_home_leave::date, null) as start_date_home_leave
    , start_time_home_leave::time as start_time_home_leave
    , iff(end_date_home_leave::date >= '1901-01-01'::date, end_date_home_leave::date, null) as end_date_home_leave
    , end_time_home_leave::time as end_time_home_leave
    , home_leave_days_end_rp
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
