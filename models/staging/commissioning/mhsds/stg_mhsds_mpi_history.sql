{{
    config(
        materialized = 'view',
        tags = ['mhsds']
    )
}}

with accepted_records as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs001mpi')) }}
)

select
    sk
    , mhs001_uniq_id
    , unique_local_patient_id
    , uniq_submission_id
    , reporting_period_end_date::date as reporting_period_end_date
    , nhs_number_pseudo as source_pseudonymised_nhs_number
    , pers_death_date
    , person_id
    , local_patient_id
    , org_id_prov
    , dmic_ccg_code
from accepted_records
