{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

SELECT
    observation.id,
    observation.person_id,
    observation.clinical_effective_date,
    observation.date_recorded
FROM ({{ get_observations("'DS_COD'", source='PCD') }}) AS observation
