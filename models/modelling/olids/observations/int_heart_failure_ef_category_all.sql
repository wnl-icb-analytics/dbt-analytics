{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH observations AS (
    SELECT * FROM ({{ get_observations("'HFREF_DX_COD', 'HFMREF_DX_COD', 'HFPEF_DX_COD'", source='ECL_CACHE') }})
    UNION ALL
    SELECT * FROM ({{ get_observations("'REDEJCFRAC_COD'", source='PCD') }})
)
SELECT
    id,
    person_id,
    clinical_effective_date,
    date_recorded,
    MAX(cluster_id IN ('HFREF_DX_COD', 'REDEJCFRAC_COD')) AS is_reduced_ef,
    MAX(cluster_id = 'HFMREF_DX_COD') AS is_mildly_reduced_ef,
    MAX(cluster_id = 'HFPEF_DX_COD') AS is_preserved_ef
FROM observations
GROUP BY id, person_id, clinical_effective_date, date_recorded
