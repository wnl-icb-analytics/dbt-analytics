{{
    config(
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Contraindications to antiplatelet and oral anticoagulant therapy from the PCD
clusters, one row per observation and cluster: salicylates (XSAL_COD,
TXSAL_COD), clopidogrel (XCLO_COD, TXCLO_COD), dipyridamole (XDIPY_COD,
TXDIPY_COD) and oral anticoagulants (XORANTICOAG_COD, TXORANTICOAG_COD,
XORANTICOAGGENCON_COD, TXORANTICOAGGENCON_COD). is_persisting distinguishes a
persisting contraindication, which QOF reads as standing, from an expiring one,
current for 12 months. Includes ALL persons (active, inactive, deceased)
following intermediate layer principles. Observations dated after the build
date are excluded.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    CASE
        WHEN obs.cluster_id IN ('XSAL_COD', 'TXSAL_COD') THEN 'SALICYLATE'
        WHEN obs.cluster_id IN ('XCLO_COD', 'TXCLO_COD') THEN 'CLOPIDOGREL'
        WHEN obs.cluster_id IN ('XDIPY_COD', 'TXDIPY_COD') THEN 'DIPYRIDAMOLE'
        ELSE 'ORAL_ANTICOAGULANT'
    END AS drug_class,
    obs.cluster_id IN ('XSAL_COD', 'XCLO_COD', 'XDIPY_COD', 'XORANTICOAG_COD', 'XORANTICOAGGENCON_COD') AS is_persisting,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'XSAL_COD', 'TXSAL_COD', 'XCLO_COD', 'TXCLO_COD', 'XDIPY_COD', 'TXDIPY_COD', 'XORANTICOAG_COD', 'TXORANTICOAG_COD', 'XORANTICOAGGENCON_COD', 'TXORANTICOAGGENCON_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
