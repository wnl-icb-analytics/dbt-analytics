{{
    config(
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Proteinuria, microalbuminuria and diabetic nephropathy records from the PCD
clusters, one row per observation and cluster. record_type names the finding
from its cluster. Includes ALL persons (active, inactive, deceased) following
intermediate layer principles. Observations dated after the build date are
excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE obs.cluster_id
        WHEN 'PRT_COD' THEN 'PROTEINURIA'
        WHEN 'CKDPRT_COD' THEN 'CKD_PROTEINURIA'
        WHEN 'NDAPRT_COD' THEN 'PERSISTENT_PROTEINURIA'
        WHEN 'MAL_COD' THEN 'MICROALBUMINURIA'
        WHEN 'DIABNEPHROP_COD' THEN 'DIABETIC_NEPHROPATHY'
    END AS record_type,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'PRT_COD', 'CKDPRT_COD', 'NDAPRT_COD', 'MAL_COD', 'DIABNEPHROP_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
