{{
     config(
        materialized = 'view',
        tags=['mhsds']
        )
}}

with accepted_records as (
{{
    select_accepted_mhsds_period_records(
        mhsds_table = ref('raw_mhsds_mhs204indirectactivity')
    )
}} )

select
    mhs204_uniq_id
    , uniq_serv_req_id
    , person_id
    , indirect_act_date
    , duration_indirect_act
    , care_prof_team_local_id
    , other_care_prof_team_local_id
    , uniq_submission_id
    , reporting_period_start_date::date as reporting_period_start_date
    , reporting_period_end_date::date as reporting_period_end_date
    , effective_from
from accepted_records
