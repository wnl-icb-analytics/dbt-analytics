{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='end_date',
        cluster_by=['end_date', 'person_id'],
        snowflake_warehouse=env_var('EFI2_HISTORY_WAREHOUSE', target.warehouse),
        tags=['efi2', 'monthly-full'])
}}

/*
Monthly eFI2 chronology. Incremental runs replace the latest completed month
and add missing newer months; the monthly full refresh recalculates 60 months.
*/

{{ efi2_chronology(
    rules_relation=ref('int_efi2_rules_history'),
    patient_list_relation=ref('int_efi2_patient_list_history'),
    weights_relation=ref('stg_common_efi2_weights'),
    medication_order_relation=ref('stg_olids_medication_order')
) }}
