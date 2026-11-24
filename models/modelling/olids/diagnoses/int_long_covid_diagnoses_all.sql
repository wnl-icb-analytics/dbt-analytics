{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Long COVID diagnosis and resolution observations from GP records. Post acute infection syndromes.
ECL clusters from the SNOMED Cluster Manager, with retired SNOMED CT predecessors:
- LONGCOVID_COD: << 404684003 |Clinical finding| : 255234002 |After| = << 840539006 |COVID-19|
- LONGCOVIDRES_COD: << 1326351000000108 |Post-COVID-19 syndrome resolved|

Includes ALL persons (active, inactive, deceased). One row per observation.
Input to fct_person_long_covid_register and calculate_long_covid_register, which apply the register rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.cluster_id = 'LONGCOVID_COD' AS is_diagnosis_code,
    obs.cluster_id = 'LONGCOVIDRES_COD' AS is_resolved_code
FROM ({{ get_observations("'LONGCOVID_COD', 'LONGCOVIDRES_COD'", source='ECL_CACHE', include_history=true) }}) AS obs
