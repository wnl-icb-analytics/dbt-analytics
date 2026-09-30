{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs505restrictiveinterventinc')) }}
)
select
    mhs505_uniq_id
    , person_id
    , uniq_restrictive_int_inc_id
    , uniq_serv_req_id
    , uniq_hosp_prov_spell_id
    , iff(start_date_restrictive_int_inc::date >= '1901-01-01'::date, start_date_restrictive_int_inc::date, null) as start_date_restrictive_int_inc
    , start_time_restrictive_int_inc::time as start_time_restrictive_int_inc
    , iff(end_date_restrictive_int_inc::date >= '1901-01-01'::date, end_date_restrictive_int_inc::date, null) as end_date_restrictive_int_inc
    , end_time_restrictive_int_inc::time as end_time_restrictive_int_inc
    , restrictive_int_reason
    , restrictive_int_pi_review_held_pat
    , restrictive_int_pi_review_not_held_reas_pat
    , restrictive_int_pi_review_held_care_pers
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
