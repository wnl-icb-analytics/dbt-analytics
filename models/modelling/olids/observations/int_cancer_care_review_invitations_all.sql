{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

SELECT
    observation.id,
    observation.person_id,
    observation.clinical_effective_date,
    observation.cluster_id AS source_cluster_id
FROM ({{ get_observations("'CANINVITE_COD'", source='PCD') }}) AS observation
