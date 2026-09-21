{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs003accommstatus')) }}
)
select
    mhs003_uniq_id
    , person_id
    , accommodation_status_code
    , accommodation_type
    , settled_accommodation_ind
    , iff(accommodation_status_date::date >= '1901-01-01'::date, accommodation_status_date::date, null) as accommodation_status_date
    , iff(accommodation_type_date::date >= '1901-01-01'::date, accommodation_type_date::date, null) as accommodation_type_date
    , sch_placement_type
    , iff(accommodation_type_start_date::date >= '1901-01-01'::date, accommodation_type_start_date::date, null) as accommodation_type_start_date
    , iff(accommodation_type_end_date::date >= '1901-01-01'::date, accommodation_type_end_date::date, null) as accommodation_type_end_date
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
