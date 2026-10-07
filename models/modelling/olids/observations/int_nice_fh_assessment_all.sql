{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH observations AS (
    SELECT * FROM ({{ get_observations("'FH_ASSESS_COD', 'FH_REFER_COD', 'SECONDARY_HYPERLIPID_COD', 'SECONDARY_HYPERLIPID_HX_COD'", source='ECL_CACHE') }})
    UNION ALL
    SELECT * FROM ({{ get_observations("'SBROOME_COD', 'DULIPID_COD', 'POSSFH_COD', 'FHYPGEN_COD', 'FHYP_COD'", source='PCD') }})
)
SELECT
    id,
    person_id,
    clinical_effective_date,
    MAX(cluster_id IN ('FH_ASSESS_COD', 'SBROOME_COD', 'DULIPID_COD', 'POSSFH_COD')) AS is_fh_assessment,
    MAX(cluster_id = 'FHYP_COD') AS is_clinical_fh_diagnosis,
    MAX(cluster_id = 'FH_REFER_COD') AS is_fh_referral,
    MAX(cluster_id = 'FHYPGEN_COD') AS is_genetic_fh,
    MAX(cluster_id = 'SECONDARY_HYPERLIPID_COD') AS is_secondary_hyperlipidaemia,
    MAX(cluster_id = 'SECONDARY_HYPERLIPID_HX_COD') AS is_secondary_hyperlipidaemia_history
FROM observations
GROUP BY id, person_id, clinical_effective_date
