{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs518clinreadyfordischarge')) }}
)
select
    mhs518_uniq_id
    , person_id
    , uniq_serv_req_id
    , uniq_hosp_prov_spell_id
    , iff(start_date_clin_readyfor_disch::date >= '1901-01-01'::date, start_date_clin_readyfor_disch::date, null) as start_date_clin_readyfor_disch
    , iff(end_date_clin_readyfor_disch::date >= '1901-01-01'::date, end_date_clin_readyfor_disch::date, null) as end_date_clin_readyfor_disch
    , clin_readyfor_disch_delay_reason
    , attrib_to_indic
    , org_id_resp_la_clin_readyfor_disch
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
