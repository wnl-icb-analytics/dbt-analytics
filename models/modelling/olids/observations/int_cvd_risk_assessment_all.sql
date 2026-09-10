{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Cardiovascular risk assessment records from the PCD clusters CVDRISKASS_COD and
QRISKSCORE_COD, one row per observation: QRISK, QRISK2, QRISK3, JBS and
Framingham scores plus "assessment done" codes. This is the single home for "an
assessment was recorded", with or without a score value; int_qrisk_all holds the
scored QRISK values. An observation matching both clusters appears once. Includes ALL persons (active, inactive, deceased) following
intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    TRY_CAST(obs.result_value AS FLOAT) AS risk_score_value,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.cluster_id = 'QRISKSCORE_COD' OR obs.mapped_concept_display ILIKE '%QRISK%' AS is_qrisk_code
FROM ({{ get_observations("'CVDRISKASS_COD', 'QRISKSCORE_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date <= CURRENT_DATE()
QUALIFY ROW_NUMBER() OVER (PARTITION BY obs.id ORDER BY obs.cluster_id) = 1
