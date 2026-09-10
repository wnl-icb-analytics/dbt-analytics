select
    appointment_id,
    clinical_record_type,
    clinical_record_id,
    source_record_id,
    encounter_id,
    patient_id,
    person_id
from {{ ref('raw_olids_appointment_clinical_record') }}
