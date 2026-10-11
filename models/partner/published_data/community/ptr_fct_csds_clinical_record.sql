{{
    config(
        cluster_by=['sk_patient_id']
    )
}}

select * replace (sk_patient_id::varchar as sk_patient_id)
from {{ ref('fct_csds_clinical_record') }}
-- Exclude social circumstances until the owner confirms the partner sharing basis.
where clinical_record_type <> 'social_circumstance'
