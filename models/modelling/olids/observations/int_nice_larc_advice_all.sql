{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}
WITH advice AS (
    SELECT id, person_id, clinical_effective_date::DATE AS event_date,
        code_description AS concept_display
    FROM ({{ get_observations("'LARC_ADV_COD'", source='ECL_CACHE') }}) AS observation
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
)
SELECT id, person_id, event_date, concept_display,
    -- The maintained list label identifies explicit modality; method education remains unknown.
    CASE
        WHEN concept_display ILIKE '%verbal%' THEN 'VERBAL'
        WHEN concept_display ILIKE '%written%' OR concept_display ILIKE '%leaflet%' THEN 'WRITTEN'
        WHEN concept_display ILIKE 'Advice about long acting reversible contraception%' THEN 'NON_SPECIFIC'
        ELSE 'UNKNOWN'
    END AS advice_modality
FROM advice
