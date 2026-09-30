{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs004empstatus')) }}
)
select
    mhs004_uniq_id
    , person_id
    , employ_status
    , iff(employ_status_start_date::date >= '1901-01-01'::date, employ_status_start_date::date, null) as employ_status_start_date
    , iff(employ_status_end_date::date >= '1901-01-01'::date, employ_status_end_date::date, null) as employ_status_end_date
    , iff(employ_status_rec_date::date >= '1901-01-01'::date, employ_status_rec_date::date, null) as employ_status_rec_date
    , pat_prim_emp_cont_type_mh
    , week_hours_worked
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
