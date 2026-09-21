{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs512hospspellcomm')) }}
)
select
    mhs512_uniq_id
    , person_id
    , uniq_serv_req_id
    , uniq_hosp_prov_spell_id
    , org_id_comm
    , iff(start_date_org_code_comm::date >= '1901-01-01'::date, start_date_org_code_comm::date, null) as start_date_org_code_comm
    , iff(end_date_org_code_comm::date >= '1901-01-01'::date, end_date_org_code_comm::date, null) as end_date_org_code_comm
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
