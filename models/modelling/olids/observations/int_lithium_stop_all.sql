{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- Lithium stop observations from LITSTP_COD.
-- One row per observation, including inactive and deceased people.
SELECT
    observation.id,
    observation.person_id,
    observation.clinical_effective_date,
    observation.date_recorded,
    observation.cluster_id AS source_cluster_id
FROM ({{ get_observations("'LITSTP_COD'", source='PCD') }}) AS observation
WHERE observation.clinical_effective_date <= CURRENT_DATE()
