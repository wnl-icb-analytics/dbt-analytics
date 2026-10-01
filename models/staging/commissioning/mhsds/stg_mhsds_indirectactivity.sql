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
    , iff(indirect_act_date::date >= '1901-01-01'::date, indirect_act_date::date, null) as indirect_act_date
    , duration_indirect_act
    , care_prof_team_local_id
    , other_care_prof_team_local_id
    , uniq_submission_id
    , reporting_period_start_date::date as reporting_period_start_date
    , reporting_period_end_date::date as reporting_period_end_date
    , indirect_act_time::time as indirect_act_time
    , ind_act_pers_cons
    , org_id_prov
    , org_id_comm
    , care_prof_local_id
    , uniq_care_prof_local_id
    , ind_act_procedure
    , find_scheme_in_use
    , finding
    , master_snomed_ct_finding_code
    , dm_icb_commissioner
    , dm_sub_icb_commissioner
    , dmic_dataset
    , effective_from
from accepted_records
