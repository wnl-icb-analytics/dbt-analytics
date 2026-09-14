{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- Shingles administration and contraindication evidence, before programme dose assignment.
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.cluster_id != 'SHCON_COD' AS is_administered,
    obs.cluster_id = 'SHCON_COD' AS is_contraindicated,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'SHVACGP_COD', 'SHVACOHP_COD', 'SHVACGP1_COD', 'SHVACGP2_COD', 'SHCON_COD'", source='PCD') }}) AS obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
