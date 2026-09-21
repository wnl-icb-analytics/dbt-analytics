{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs104rtt')) }}
)
select
    mhs104_uniq_id
    , person_id
    , uniq_serv_req_id
    , pat_path_id_pseudo
    , wait_time_measure_type
    , org_id_pat_path_id_issuer
    , iff(refer_to_treat_period_start_date::date >= '1901-01-01'::date, refer_to_treat_period_start_date::date, null) as refer_to_treat_period_start_date
    , iff(refer_to_treat_period_end_date::date >= '1901-01-01'::date, refer_to_treat_period_end_date::date, null) as refer_to_treat_period_end_date
    , refer_to_treat_period_status
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
