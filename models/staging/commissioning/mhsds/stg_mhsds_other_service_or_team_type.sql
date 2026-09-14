{{ config(materialized='table', tags=['mhsds']) }}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs102servicetypereferredto')) }}
)

select
    s.mhs102_uniq_id
    , s.uniq_serv_req_id
    , s.service_request_id
    , s.person_id
    , s.org_id_prov
    , s.care_prof_team_local_id
    , s.other_care_prof_team_local_id
    , s.uniq_care_prof_team_id
    , s.uniq_other_care_prof_team_local_id
    , coalesce(
        s.uniq_other_care_prof_team_local_id
        , s.uniq_care_prof_team_id
        , s.other_care_prof_team_local_id
        , s.care_prof_team_local_id
    ) as service_or_team_id
    , s.serv_team_type_ref_to_mh
    , s.service_type_name
    , s.serv_team_int_age_group
    , s.refer_rejection_date::date as refer_rejection_date
    , s.refer_rejection_time::time as refer_rejection_time
    , s.refer_reject_reason
    , s.refer_closure_date::date as refer_closure_date
    , s.refer_closure_time::time as refer_closure_time
    , s.refer_clos_reason
    , s.age_serv_refer_rejection
    , s.age_serv_refer_closure
    , s.record_start_date::date as record_start_date
    , s.record_end_date::date as record_end_date
    , s.uniq_submission_id
    , s.uniq_month_id
    , s.reporting_period_start_date::date as reporting_period_start_date
    , s.reporting_period_end_date::date as reporting_period_end_date
    , s.dmic_dataset
    , s.effective_from
    , s.dmic_date_added
from accepted_records as s
where service_or_team_id is not null
qualify row_number() over (
    partition by s.uniq_serv_req_id, service_or_team_id
    order by
        s.reporting_period_end_date desc
        , s.effective_from desc nulls last
        , s.uniq_submission_id desc
        , s.row_number desc
        , s.mhs102_uniq_id desc
) = 1
