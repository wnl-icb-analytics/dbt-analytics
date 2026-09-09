{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Records that limit oral anticoagulant or DOAC treatment, from PCD clusters, one
row per observation. exception_type separates:
- ANTICOAGULANT_ADVERSE_REACTION: ORANTICOAGADVERS_COD (allergy or adverse reaction
  to any oral anticoagulant; persisting)
- ANTICOAGULANT_CONTRAINDICATED: TXORANTICOAGGENCON_COD (anticoagulation
  contraindicated or not tolerated; expiring)
- ANTICOAGULANT_DECLINED: ORANTICOAGDEC_COD
- DOAC_CONTRAINDICATED: DOACCON_COD
- DOAC_DECLINED: DOACDEC_COD
- DOAC_NOT_INDICATED: DOACNI_COD
- VALVULAR_AF: AF_VALVULAR_OBS (local ECL cluster: mitral stenosis and mechanical
  valve findings where a DOAC is not indicated)
- ANTIPHOSPHOLIPID_SYNDROME: ANTIPHOSYND_COD
The broad QOF cluster TXORANTICOAG_COD is not used: it mixes drug-specific "not
indicated" codes that are recorded when a person is on the other anticoagulant.
Consumers decide which types persist and which expire. Includes ALL persons (active, inactive, deceased) following
intermediate layer principles.
*/

WITH pcd AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        CASE obs.cluster_id
            WHEN 'ORANTICOAGADVERS_COD' THEN 'ANTICOAGULANT_ADVERSE_REACTION'
            WHEN 'TXORANTICOAGGENCON_COD' THEN 'ANTICOAGULANT_CONTRAINDICATED'
            WHEN 'ORANTICOAGDEC_COD' THEN 'ANTICOAGULANT_DECLINED'
            WHEN 'DOACCON_COD' THEN 'DOAC_CONTRAINDICATED'
            WHEN 'DOACDEC_COD' THEN 'DOAC_DECLINED'
            WHEN 'DOACNI_COD' THEN 'DOAC_NOT_INDICATED'
            WHEN 'ANTIPHOSYND_COD' THEN 'ANTIPHOSPHOLIPID_SYNDROME'
        END AS exception_type,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'ORANTICOAGADVERS_COD', 'TXORANTICOAGGENCON_COD', 'ORANTICOAGDEC_COD', 'DOACCON_COD', 'DOACDEC_COD', 'DOACNI_COD', 'ANTIPHOSYND_COD'", source='PCD') }}) obs
),

valvular AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        'VALVULAR_AF' AS exception_type,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'AF_VALVULAR_OBS'", source='ECL_CACHE') }}) obs
)

SELECT * FROM pcd
WHERE clinical_effective_date <= CURRENT_DATE()
UNION ALL
SELECT * FROM valvular
WHERE clinical_effective_date <= CURRENT_DATE()
