{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Fibromyalgia diagnosis observations from GP records. Post acute infection syndromes.
ECL clusters from the SNOMED Cluster Manager, with retired SNOMED CT predecessors:
- FIBROMYALGIA_COD: << 203082005 |Fibromyalgia|

Includes ALL persons (active, inactive, deceased). One row per observation.
Input to fct_person_fibromyalgia_register and calculate_fibromyalgia_register, which apply the register rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.cluster_id = 'FIBROMYALGIA_COD' AS is_diagnosis_code
FROM ({{ get_observations("'FIBROMYALGIA_COD'", source='ECL_CACHE', include_history=true) }}) AS obs
