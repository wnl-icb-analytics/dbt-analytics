{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH evidence AS (
SELECT observation.id, observation.person_id, observation.clinical_effective_date,
    observation.date_recorded, observation.cluster_id AS source_cluster_id
FROM ({{ get_observations("'HIV_COD','AIDS_COD'", source="PCD") }}) AS observation
)
SELECT id, person_id, clinical_effective_date, date_recorded, source_cluster_id
FROM evidence
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_cluster_id) = 1
