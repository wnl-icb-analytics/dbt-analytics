{{
    config(
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Contraindications to ACE inhibitors and angiotensin II receptor blockers from
the PCD clusters, one row per observation and cluster. drug_class names the
class; is_persisting distinguishes a persisting contraindication (XACE_COD,
XAII_COD) from an expiring one (TXACE_COD, TXAII_COD), which NICE reads as
current for 12 months. Includes ALL persons (active, inactive, deceased)
following intermediate layer principles. Observations dated after the build
date are excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE obs.cluster_id
        WHEN 'XACE_COD' THEN 'ACE_INHIBITOR'
        WHEN 'TXACE_COD' THEN 'ACE_INHIBITOR'
        WHEN 'XAII_COD' THEN 'ARB'
        WHEN 'TXAII_COD' THEN 'ARB'
    END AS drug_class,
    obs.cluster_id IN ('XACE_COD', 'XAII_COD') AS is_persisting,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'XACE_COD', 'TXACE_COD', 'XAII_COD', 'TXAII_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
