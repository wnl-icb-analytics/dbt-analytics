{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All hypothyroidism diagnosis observations from clinical records.
Uses hypothyroidism cluster IDs:
- HYPOTHY_COD: ECL-based hypothyroidism diagnoses (<< 40930008 |Hypothyroidism|, 113 concepts)
- THY_COD: Legacy hypothyroidism diagnoses (retained for backward compatibility)

Hypothyroidism is a chronic endocrine condition with no resolution codes.
No age restrictions applied.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per hypothyroidism observation.
Use this model as input for fct_person_hypothyroidism_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Hypothyroidism-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- Hypothyroidism observation type
    'Hypothyroidism Diagnosis' AS hypothyroidism_observation_type

FROM ({{ get_observations("'HYPOTHY_COD', 'THY_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
-- Deduplicate overlapping clusters: same observation may match both HYPOTHY_COD and THY_COD
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY obs.id
    ORDER BY CASE WHEN obs.cluster_id = 'HYPOTHY_COD' THEN 0 ELSE 1 END
) = 1

ORDER BY person_id, clinical_effective_date, id

