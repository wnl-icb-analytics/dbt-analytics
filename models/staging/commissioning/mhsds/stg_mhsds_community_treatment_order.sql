{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs404commtreatorder')) }}
)
select
    mhs404_uniq_id
    , person_id
    , uniq_mh_act_episode_id
    , iff(start_date_comm_treat_ord::date >= '1901-01-01'::date, start_date_comm_treat_ord::date, null) as start_date_comm_treat_ord
    , iff(expiry_date_comm_treat_ord::date >= '1901-01-01'::date, expiry_date_comm_treat_ord::date, null) as expiry_date_comm_treat_ord
    , iff(end_date_comm_treat_ord::date >= '1901-01-01'::date, end_date_comm_treat_ord::date, null) as end_date_comm_treat_ord
    , comm_treat_ord_end_reason
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
