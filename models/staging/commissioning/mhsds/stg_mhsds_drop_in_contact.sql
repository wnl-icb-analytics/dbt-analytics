{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs302mhdropincontact')) }}
)
select
    mhs302_uniq_id
    , uniq_mh_drop_in_contact_id
    , iff(care_contact_date_mh_drop_in_contact::date >= '1901-01-01'::date, care_contact_date_mh_drop_in_contact::date, null) as care_contact_date_mh_drop_in_contact
    , org_id_comm
    , mh_drop_in_contact_service_type
    , start_time_drop_in_contact::time as start_time_drop_in_contact
    , end_time_drop_in_contact::time as end_time_drop_in_contact
    , gender_id_code
    , gender_same_at_birth
    , ethnic_category
    , ethnic_category2021
    , cons_mechanism_mh
    , uniq_care_prof_local_id
    , mh_drop_in_contact_outcome
    , org_id_receiving
    , age_rep_period_start
    , age_rep_period_end
    , dm_icb_commissioner
    , dm_sub_icb_commissioner
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
