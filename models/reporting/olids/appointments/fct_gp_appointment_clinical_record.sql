{{ config(materialized='view') }}

select
    r.appointment_id,
    r.clinical_record_type,
    r.clinical_record_id,
    r.source_record_id,
    r.encounter_id,
    r.person_id,
    p.sk_patient_id,
    r.patient_id
from {{ ref('stg_olids_appointment_clinical_record') }} as r
left join {{ ref('dim_person_pseudo') }} as p
    on r.person_id = p.person_id
