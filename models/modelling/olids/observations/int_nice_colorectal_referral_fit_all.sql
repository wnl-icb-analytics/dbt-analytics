{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

WITH screening_codes AS (
    SELECT DISTINCT code
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE source = 'PCD' AND cluster_id = 'COLCANSCREV_COD'
)
SELECT id::VARCHAR AS id, person_id, clinical_effective_date::DATE AS event_date, cluster_id AS source_cluster_id
FROM ({{ get_observations("'GICANREF_COD', 'FAECIMM_COD'", source='PCD') }}) AS observation
WHERE clinical_effective_date::DATE <= CURRENT_DATE()
    AND (cluster_id <> 'FAECIMM_COD' OR NOT EXISTS (
        SELECT 1 FROM screening_codes
        WHERE screening_codes.code = observation.mapped_concept_code
    ))
