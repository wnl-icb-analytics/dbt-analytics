{{ config(materialized='table', tags=['mhsds']) }}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs206staffactivity')) }}
)

select
    s.mhs206_uniq_id
    , s.uniq_care_act_id
    , s.care_act_id
    , s.uniq_care_prof_local_id
    , s.care_prof_local_id
    , s.uniq_care_cont_id
    , s.uniq_serv_req_id
    , s.person_id
    , s.org_id_prov
    , s.record_number as source_record_number
    , s.row_number as source_row_number
    , s.uniq_submission_id
    , s.uniq_month_id
    , s.reporting_period_start_date::date as reporting_period_start_date
    , s.reporting_period_end_date::date as reporting_period_end_date
    , s.dmic_dataset as mhsds_version
    , s.effective_from as source_file_received_at
    , s.dmic_date_added as source_loaded_at
from accepted_records as s
