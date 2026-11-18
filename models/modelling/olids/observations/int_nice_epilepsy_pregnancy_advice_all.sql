{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

SELECT id, person_id, clinical_effective_date::DATE AS event_date, cluster_id AS source_cluster_id
FROM ({{ get_observations("'EPILCC_COD', 'EPILPA_COD', 'EPILPCA_COD'", source='PCD') }}) AS observation
WHERE clinical_effective_date::DATE <= CURRENT_DATE()
