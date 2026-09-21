{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs515restrictiveinterventtype')) }}
)
select
    mhs515_uniq_id
    , person_id
    , uniq_restrictive_int_inc_id
    , uniq_restrictive_int_type_id
    , uniq_serv_req_id
    , uniq_hosp_prov_spell_id
    , uniq_ward_stay_id
    , restrictive_int_type
    , iff(start_date_restrictive_int_type::date >= '1901-01-01'::date, start_date_restrictive_int_type::date, null) as start_date_restrictive_int_type
    , start_time_restrictive_int_type::time as start_time_restrictive_int_type
    , iff(end_date_restrictive_int_type::date >= '1901-01-01'::date, end_date_restrictive_int_type::date, null) as end_date_restrictive_int_type
    , end_time_restrictive_int_type::time as end_time_restrictive_int_type
    , restraint_injury_patient
    , restraint_injury_care_pers
    , restraint_injury_other_pers
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
