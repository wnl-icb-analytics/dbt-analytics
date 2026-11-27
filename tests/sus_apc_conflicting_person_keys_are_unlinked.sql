-- Neither disputed identity is authoritative until the business owner decides.
select
    clinical_record_id,
    source_record_type
from {{ ref('int_sus_person_clinical_record') }}
where source_dataset = 'SUS_APC'
    and is_parent_person_consistent = false
    and sk_patient_id is not null
