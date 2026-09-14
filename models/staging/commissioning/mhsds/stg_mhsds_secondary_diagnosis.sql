{{ config(materialized='table', tags=['mhsds']) }}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs605secdiag')) }}
)

select
    mhs605_uniq_id
    , uniq_serv_req_id
    , diag_scheme_in_use
    , sec_diag
    , coded_diag_timestamp
    , diag_date
    , master_snomed_ct_sec_diag_code
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
