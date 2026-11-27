{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Post-viral fatigue syndrome diagnosis observations from GP records. Post acute infection syndromes.
ECL clusters from the SNOMED Cluster Manager, with retired SNOMED CT predecessors:
- PVFS_COD: << 51771007 |Postviral fatigue syndrome|

Includes ALL persons (active, inactive, deceased). One row per observation.
Input to fct_person_post_viral_fatigue_register and calculate_post_viral_fatigue_register, which apply the register rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.cluster_id = 'PVFS_COD' AS is_diagnosis_code
FROM ({{ get_observations("'PVFS_COD'", source='ECL_CACHE', include_history=true) }}) AS obs
