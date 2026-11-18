{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- NICE alcohol-disorder exclusion observations from ALCOHOL_MISUSE_DISORDERS, with no age-at-event floor.
-- One row per observation, including inactive and deceased people.
SELECT
    observation.id,
    observation.person_id,
    observation.clinical_effective_date,
    observation.date_recorded,
    observation.cluster_id AS source_cluster_id
FROM ({{ get_observations("'ALCOHOL_MISUSE_DISORDERS'") }}) AS observation
WHERE observation.clinical_effective_date <= CURRENT_DATE()
