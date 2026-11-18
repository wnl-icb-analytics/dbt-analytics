{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

WITH topics AS (
    SELECT id, person_id, clinical_effective_date::DATE AS event_date,
        cluster_id AS source_cluster_id, 'ECL_CACHE' AS source,
        cluster_id = 'CONTRACEPTION_ADVICE_TOPIC_COD' AS is_contraception_topic,
        cluster_id = 'PREGNANCY_ADVICE_TOPIC_COD' AS is_pregnancy_topic
    FROM ({{ get_observations("'CONTRACEPTION_ADVICE_TOPIC_COD', 'PREGNANCY_ADVICE_TOPIC_COD'", source='ECL_CACHE') }}) AS observation
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
    UNION ALL
    SELECT id, person_id, event_date, source_cluster_id, 'PCD' AS source,
        source_cluster_id = 'EPILCC_COD' AS is_contraception_topic,
        source_cluster_id IN ('EPILPA_COD', 'EPILPCA_COD') AS is_pregnancy_topic
    FROM {{ ref('int_nice_epilepsy_pregnancy_advice_all') }}
)
SELECT id, person_id, event_date, source_cluster_id, source,
    is_contraception_topic, is_pregnancy_topic
FROM topics
