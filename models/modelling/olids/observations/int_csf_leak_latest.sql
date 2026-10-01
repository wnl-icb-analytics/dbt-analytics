{{
    config(
       materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['adult_imms'])
}}
 --This model captures the latest observation for a PPV vaccination clinical risk group for Cerebrospinal fluid leak codes.
SELECT
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display
FROM (
    {{ get_latest_events(
        ref('int_csf_leak_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_csf