{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

SELECT person_id, id AS observation_id, clinical_effective_date::DATE AS event_date
FROM ({{ get_observations("'SMOK_COD'", source='PCD') }})
WHERE clinical_effective_date::DATE <= CURRENT_DATE()
