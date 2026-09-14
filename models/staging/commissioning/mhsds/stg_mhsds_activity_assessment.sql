{{ config(materialized='table', tags=['mhsds']) }}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs607codedscoreassessmentact')) }}
)

select
    mhs607_uniq_id
    , uniq_serv_req_id
    , uniq_care_cont_id
    , uniq_care_act_id
    , coded_ass_tool_type
    , pers_score
    , org_id_prov
    , person_id
    , uniq_submission_id
    , uniq_month_id
    , record_number
    , row_number
    , reporting_period_start_date
    , reporting_period_end_date
    , effective_from
    , dmic_date_added
    , dmic_dataset
from accepted_records
