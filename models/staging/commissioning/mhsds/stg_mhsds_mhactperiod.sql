{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with deduplicated as (
    {{
        select_latest_mhsds_record(
            mhsds_table = ref('raw_mhsds_mhs401mhactperiod'),
            partition_cols = ['uniq_mh_act_episode_id'],
            tie_breaker_cols = ['mhs401_uniq_id']
        )
    }}
)

-- REVIEW: raw MHS401 has no uniq_serv_req_id, so legal-status periods cannot
-- be linked directly to a referral.
select
    mhs401_uniq_id
    , uniq_mh_act_episode_id
    , person_id
    , nhsd_legal_status
    -- 1899/1900 values are source missing-date sentinels from Excel epochs.
    , iff(
        start_date_mh_act_legal_status_class::date < '1901-01-01'::date
        , null
        , start_date_mh_act_legal_status_class::date
    ) as start_date_mh_act_legal_status_class
    , iff(
        end_date_mh_act_legal_status_class::date < '1901-01-01'::date
        , null
        , end_date_mh_act_legal_status_class::date
    ) as end_date_mh_act_legal_status_class
    , iff(
        expiry_date_mh_act_legal_status_class::date < '1901-01-01'::date
        , null
        , expiry_date_mh_act_legal_status_class::date
    ) as expiry_date_mh_act_legal_status_class
    , org_id_prov
    , unique_local_patient_id
    , uniq_submission_id
    , reporting_period_end_date::date as reporting_period_end_date
    , effective_from
from deduplicated
