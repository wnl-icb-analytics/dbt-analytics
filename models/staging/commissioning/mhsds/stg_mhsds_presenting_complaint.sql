{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs609prescomp')) }}
)
select
    mhs609_uniq_id
    , person_id
    , uniq_serv_req_id
    , find_scheme_in_use
    , pres_comp
    , pres_comp_cod_sig
    , iff(pres_comp_date::date >= '1901-01-01'::date, pres_comp_date::date, null) as pres_comp_date
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
