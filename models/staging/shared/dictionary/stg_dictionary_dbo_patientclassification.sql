select
    sk_patient_classification_id,
    bk_patient_classification_code,
    patient_classification_name,
    patient_classification_full_name
from {{ ref('raw_dictionary_dbo_patientclassification') }}