{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}
WITH advice AS (
    SELECT id, person_id, clinical_effective_date::DATE AS event_date,
        cluster_id AS source_cluster_id, 'ECL_CACHE' AS source
    FROM ({{ get_observations("'CONTRACEP_ADV_COD', 'LARC_ADV_COD'", source='ECL_CACHE') }}) AS observation
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
    UNION ALL
    SELECT id, person_id, clinical_effective_date::DATE AS event_date,
        cluster_id AS source_cluster_id, 'PCD' AS source
    FROM ({{ get_observations("'SEXHEALTHINT_COD'", source='PCD') }}) AS observation
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
    UNION ALL
    SELECT id, person_id, event_date, source_cluster_id, 'PCD' AS source
    FROM {{ ref('int_nice_epilepsy_pregnancy_advice_all') }}
)
SELECT id, person_id, event_date, source_cluster_id, source
FROM advice
