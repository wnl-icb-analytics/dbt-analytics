{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All hypertensive disorders of pregnancy (HDP) observations from clinical records.
Uses the HYPERTENSIVE_DISORDERS_OF_PREGNANCY cluster (ECL-managed) covering:
  - Chronic hypertension in pregnancy
  - Gestational hypertension
  - Pre-eclampsia (including HELLP)
  - Eclampsia and variants
  - Historical and inactive SNOMED concepts

Observation-level data - one row per HDP-related observation.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.patient_id,

    TRUE AS is_hdp_code

FROM ({{ get_observations("'HYPERTENSIVE_DISORDERS_OF_PREGNANCY'", source='ECL_CACHE', include_history=true) }}) obs

ORDER BY person_id, clinical_effective_date, id
