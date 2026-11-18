{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- Completed non-numeric lipid tests for NICE physical-health recording, including inactive and deceased people.
SELECT
    id,
    person_id,
    clinical_effective_date,
    date_recorded,
    cluster_id AS source_cluster_id
FROM ({{ get_observations("'NONVALCHOL_COD'", source='PCD') }}) AS observation
WHERE clinical_effective_date::DATE <= CURRENT_DATE()
