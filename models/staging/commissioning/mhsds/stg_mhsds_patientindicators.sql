{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs005patind')) }}
)

select
    mhs005_uniq_id
    , unique_local_patient_id
    , person_id
    , cpp
    , lac_status
    , lac_legal_status
    , org_id_prov
    , uniq_submission_id
    , reporting_period_end_date
    , effective_from
from accepted_records
