{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['efi2'])
}}

/*
Current eFI2 deficit rules. The shared macro also powers the monthly history so
threshold and precedence changes cannot drift between the two outputs.
*/

{{ efi2_rules(
    cohort_relation=ref('int_efi2_cohort_snomed_codes'),
    haemoglobin_relation=ref('int_haemoglobin_latest')
) }}
