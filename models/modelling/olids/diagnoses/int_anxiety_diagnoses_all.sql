{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All anxiety diagnosis observations from clinical records.
Uses anxiety cluster IDs:
- ANX_COD: Anxiety diagnoses
- ANXRES_COD: Anxiety resolved/remission codes

Clinical Purpose:
- Anxiety register data collection
- Mental health care pathway monitoring
- Anxiety severity tracking
- Resolution status tracking

Clinical Context:
Anxiety register includes persons with anxiety diagnosis codes who may or may not
have been resolved. Resolution logic applied in downstream fact models.
No age restrictions applied - condition can occur at any age.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per anxiety observation.
Use this model as input for fct_person_anxiety_register.sql which applies business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Anxiety-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'ANX_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'ANXRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,

    -- Anxiety observation type determination
    CASE
        WHEN obs.cluster_id = 'ANX_COD' THEN 'Anxiety Diagnosis'
        WHEN obs.cluster_id = 'ANXRES_COD' THEN 'Anxiety Resolved'
        ELSE 'Unknown'
    END AS anxiety_observation_type

FROM ({{ get_observations("'ANX_COD', 'ANXRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id

