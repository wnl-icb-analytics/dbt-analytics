{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs301groupsession')) }}
)
select
    mhs301_uniq_id
    , uniq_group_sess_id
    , iff(group_sess_date::date >= '1901-01-01'::date, group_sess_date::date, null) as group_sess_date
    , org_id_comm
    , clin_cont_dur_of_group_sess
    , group_sess_type
    , number_of_group_sess_particip
    , act_loc_type_code
    , site_id_of_treat
    , uniq_care_prof_local_id
    , serv_team_type_ref_to_mh
    , dm_icb_commissioner
    , dm_sub_icb_commissioner
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
