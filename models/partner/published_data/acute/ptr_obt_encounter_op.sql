{{
    config(
        cluster_by=['sk_patient_id']
    )
}}

select * exclude (consultant_code)
replace (sk_patient_id::varchar as sk_patient_id)
from {{ ref('obt_encounter_op') }}
