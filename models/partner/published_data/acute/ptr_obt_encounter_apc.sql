{{
    config(
        cluster_by=['sk_patient_id']
    )
}}

select * replace (sk_patient_id::varchar as sk_patient_id)
from {{ ref('obt_encounter_apc') }}
