{{
    config(
       materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['adult_imms'])
}}
--This model captures the latest observation for a PPV vaccination clinical risk group for Cochlear Implant.
SELECT
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display
FROM {{ ref('int_cochlear_implant_all') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
