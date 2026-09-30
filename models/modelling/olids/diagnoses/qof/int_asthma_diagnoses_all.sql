{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All asthma diagnoses from clinical records.
Uses QOF cluster IDs AST_COD (diagnosis) and ASTRES_COD (resolved).

Clinical Purpose:
- Asthma register inclusion for QOF respiratory disease management
- Respiratory care pathway identification and monitoring
- Childhood and adult asthma management tracking

QOF Context:
Asthma register includes persons with asthma diagnosis codes who have not
been resolved. Resolution logic applied in downstream fact models.
Age restrictions (5+ years for QOF v51) are applied in the fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for asthma register and respiratory care models.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Asthma-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'AST_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'ASTRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code

FROM ({{ get_observations("'AST_COD', 'ASTRES_COD'", source='PCD') }}) obs

ORDER BY person_id, clinical_effective_date, id
