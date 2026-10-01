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
Monthly eFI2 deficit rules. Haemoglobin is selected as at each month-end. The
final de-duplication removes repeated month-expanded rule results before they
are stored; chronology already de-duplicates these score keys, so scores do not
change. Incremental runs replace the latest completed month and add any missing
newer months.
*/

{{ efi2_rules(
    cohort_relation=ref('int_efi2_cohort_snomed_codes_history'),
    haemoglobin_relation=ref('int_haemoglobin_all'),
    historical=true,
    deduplicate_output=true,
    evidence_tiebreak_expression='evidence_id'
) }}
