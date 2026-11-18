{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- Recorded taken antithrombotic treatment for all people; one observation/cluster row.
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.clinical_effective_date_raw,
    obs.date_recorded,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'OSAL_COD', 'CLO_COD', 'ORANTICOAG_COD'", source='PCD') }}) AS obs
WHERE obs.clinical_effective_date::DATE BETWEEN '1990-01-01' AND CURRENT_DATE()
