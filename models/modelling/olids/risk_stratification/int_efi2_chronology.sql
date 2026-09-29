{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['efi2'])
}}

/*
Current eFI2 chronology. The shared macro also powers the monthly history so
dementia, alcohol, BMI, smoking and polypharmacy rules stay aligned.
*/

{{ efi2_chronology(
    rules_relation=ref('int_efi2_rules'),
    patient_list_relation=ref('int_efi2_patient_list'),
    weights_relation=ref('stg_common_efi2_weights'),
    medication_order_relation=ref('stg_olids_medication_order')
) }}
