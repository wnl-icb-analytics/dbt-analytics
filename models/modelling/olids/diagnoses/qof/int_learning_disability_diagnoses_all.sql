{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All learning disability diagnosis observations from clinical records.
Uses QOF learning disability cluster IDs:
- LD_COD: Learning disability diagnoses
- LDREM_COD: Learning disability exclusion codes (removed from register)

Clinical Purpose:
- QOF learning disability register data collection
- Learning disability support and monitoring
- Annual health checks eligibility

QOF Context (v51):
Learning disability register includes persons with LD diagnosis who have not been
excluded (LDREM_COD after LD_COD). No age restriction in QOF spec.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per learning disability observation.
Use this model as input for fct_person_learning_disability_register.sql which applies QOF business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Learning disability-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'LD_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'LDREM_COD' THEN TRUE ELSE FALSE END AS is_exclusion_code,

    -- Learning disability observation type determination
    CASE
        WHEN obs.cluster_id = 'LD_COD' THEN 'Learning Disability Diagnosis'
        WHEN obs.cluster_id = 'LDREM_COD' THEN 'Learning Disability Exclusion'
        ELSE 'Unknown'
    END AS learning_disability_observation_type

FROM ({{ get_observations("'LD_COD', 'LDREM_COD'", source='PCD') }}) obs

ORDER BY person_id, clinical_effective_date, id
