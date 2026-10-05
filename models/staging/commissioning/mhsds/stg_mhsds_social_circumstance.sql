{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs011socpercircumstances')) }}
)
select
    mhs011_uniq_id
    , person_id
    , soc_per_circumstance
    , iff(soc_per_circumstance_rec_timestamp::date >= '1901-01-01'::date, soc_per_circumstance_rec_timestamp::timestamp_ntz, null) as soc_per_circumstance_rec_timestamp
    , iff(soc_per_circumstance_rec_date::date >= '1901-01-01'::date, soc_per_circumstance_rec_date::date, null) as soc_per_circumstance_rec_date
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
