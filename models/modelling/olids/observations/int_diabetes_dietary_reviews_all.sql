{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH evidence AS (
SELECT observation.id, observation.person_id, observation.clinical_effective_date,
    observation.date_recorded, observation.cluster_id AS source_cluster_id
FROM ({{ get_observations("'DM_DIET_REVIEW_COD'", source="ECL_CACHE") }}) AS observation
UNION ALL
SELECT observation.id, observation.person_id, observation.clinical_effective_date,
    observation.date_recorded, observation.cluster_id AS source_cluster_id
FROM ({{ get_observations("'NUTRIASS_COD'", source="PCD") }}) AS observation
UNION ALL
SELECT observation.id, observation.person_id, observation.clinical_effective_date,
    observation.date_recorded, observation.cluster_id AS source_cluster_id
FROM ({{ get_observations("'NHSD_PRIMARY_CARE_DOMAIN_REFSETS/DIETITIAN_COD'", source="OPENCODELISTS") }}) AS observation
)
SELECT id, person_id, clinical_effective_date, source_cluster_id
FROM evidence
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_cluster_id) = 1
