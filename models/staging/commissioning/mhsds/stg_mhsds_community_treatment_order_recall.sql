{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs405commtreatorderrecall')) }}
)
select
    mhs405_uniq_id
    , person_id
    , uniq_mh_act_episode_id
    , iff(start_date_comm_treat_ord_recall::date >= '1901-01-01'::date, start_date_comm_treat_ord_recall::date, null) as start_date_comm_treat_ord_recall
    , start_time_comm_treat_ord_recall::time as start_time_comm_treat_ord_recall
    , iff(end_date_comm_treat_ord_recall::date >= '1901-01-01'::date, end_date_comm_treat_ord_recall::date, null) as end_date_comm_treat_ord_recall
    , end_time_comm_treat_ord_recall::time as end_time_comm_treat_ord_recall
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
    , row_number
from accepted
