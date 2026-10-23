{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs105onwardreferral')) }}
)
select
    mhs105_uniq_id
    , person_id
    , uniq_serv_req_id
    , service_request_id
    , iff(decision_to_refer_date::date >= '1901-01-01'::date, decision_to_refer_date::date, null) as decision_to_refer_date
    , decision_to_refer_time::time as decision_to_refer_time
    , iff(onward_refer_date::date >= '1901-01-01'::date, onward_refer_date::date, null) as onward_refer_date
    , onward_refer_time::time as onward_refer_time
    , nullif(upper(trim(onward_refer_reason)), '') as onward_refer_reason
    , nullif(upper(trim(oat_reason)), '') as oat_reason
    , nullif(upper(trim(org_id_receiving)), '') as org_id_receiving
    , referral_proc
    , code_ref_proc_and_proc_status
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
