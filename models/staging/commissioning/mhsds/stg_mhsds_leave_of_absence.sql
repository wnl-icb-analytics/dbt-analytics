{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs510leaveofabsence')) }}
)
select
    mhs510_uniq_id
    , person_id
    , uniq_serv_req_id
    , uniq_ward_stay_id
    , uniq_hosp_prov_spell_id
    , iff(start_date_mh_leave_abs::date >= '1901-01-01'::date, start_date_mh_leave_abs::date, null) as start_date_mh_leave_abs
    , start_time_mh_leave_abs::time as start_time_mh_leave_abs
    , iff(end_date_mh_leave_abs::date >= '1901-01-01'::date, end_date_mh_leave_abs::date, null) as end_date_mh_leave_abs
    , end_time_mh_leave_abs::time as end_time_mh_leave_abs
    , mh_leave_abs_end_reason
    , escorted_leave_indicator
    , loa_days_rp
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
