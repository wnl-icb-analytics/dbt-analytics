{{ config(materialized='view', tags=['mhsds']) }}

with active_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs902serviceteamdetails')) }}
)

select
    t.mhs902_uniq_id
    , t.care_prof_team_local_id
    , t.uniq_care_prof_team_local_id
    , t.org_id_care_prof_team_local_id
    , t.org_id_prov
    , t.serv_team_type_mh
    , t.service_type_name
    , t.serv_team_int_age_group
    , t.uniq_submission_id
    , t.uniq_month_id
    , t.reporting_period_start_date::date as reporting_period_start_date
    , t.reporting_period_end_date::date as reporting_period_end_date
    , t.dmic_dataset
    , t.effective_from
    , t.dmic_date_added
from active_records as t
