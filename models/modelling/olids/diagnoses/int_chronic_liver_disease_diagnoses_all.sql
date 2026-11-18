{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All chronic liver disease diagnosis observations from clinical records.
Uses CLD cluster IDs:
- CLDATRISK1_COD: Chronic liver disease diagnoses
- CIRRHOSIS_COD: Liver cirrhosis diagnoses (end-stage progression flag)

No resolution codes available — diagnosis-only register.
No age restrictions applied.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per CLD observation.
Use this model as input for fct_person_chronic_liver_disease_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- CLD-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'CIRRHOSIS_COD' THEN TRUE ELSE FALSE END AS is_cirrhosis_code,

    -- CLD observation type determination
    CASE
        WHEN obs.cluster_id = 'CIRRHOSIS_COD' THEN 'Cirrhosis Diagnosis'
        ELSE 'Chronic Liver Disease Diagnosis'
    END AS cld_observation_type

FROM ({{ get_observations("'CLDATRISK1_COD', 'CIRRHOSIS_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
-- Deduplicate overlapping clusters: same observation may match both clusters.
-- Prefer CIRRHOSIS_COD so the is_cirrhosis_code flag is preserved.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY obs.id
    ORDER BY CASE WHEN obs.cluster_id = 'CIRRHOSIS_COD' THEN 0 ELSE 1 END
) = 1

ORDER BY person_id, clinical_effective_date, id
