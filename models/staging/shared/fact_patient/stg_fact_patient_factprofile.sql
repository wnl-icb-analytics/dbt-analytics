-- models/staging/shared/fact_patient/stg_fact_patient_factprofile.sql
-- for use in Community CSDS Care Contact model
select
    sk_data_source_id,
    sk_patient_id,
    period_start,
    period_end,
    date_of_birth,
    sk_gender_id,
    sk_ethnicity_id,
    date_of_death

from {{ ref('raw_fact_patient_factprofile') }}
