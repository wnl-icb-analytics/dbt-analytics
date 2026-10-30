{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
         tags=['screening_programme'],
        persist_docs={"relation": true})
}}

/*
All bowel screening programme observations from clinical records.
Uses PCD REFSET cbowel screening cluster IDs:
- COLCANSCREV_COD OR COLCANSCR_COD: Bowel screening completed codes
- COLCANSCRDEC_COD: Bowel screening declined codes

Clinical Purpose:
- Bowel screening programme data collection
- Observation-level screening events tracking
- Foundation data for programme analysis

Key Business Rules:
- People aged 50 to 74 : invited every 2 years
- Declined/non-response status: valid for 12 months only
- Unsuitable status: permanent unless superseded by completed screening

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per bowel screening observation.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.result_value,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Screening type classification
    -- 368481000000103 is an observable entity recorded without result data; reclassify as Screening Recorded (No Result)
    CASE
        WHEN obs.cluster_id in ('COLCANSCREV_COD','COLCANSCR_COD') AND obs.mapped_concept_code = '368481000000103' THEN 'Screening Recorded (No Result)'
        WHEN obs.cluster_id in ('COLCANSCREV_COD','COLCANSCR_COD') THEN 'Bowel Screening Completed'
        WHEN obs.cluster_id = 'COLCANSCRDEC_COD' THEN 'Screening Declined'
        ELSE 'Unknown'
    END AS screening_observation_type,
    --no PCD refsets for either invitation non reposnse or unsuitable for bowel screening

    -- Simple screening type flags based on core cluster codes
    -- Exclude 368481000000103 (observable entity "BCSP: FOB result") which is recorded
    -- without actual result data and inflates the screening numerator vs Fingertips
    CASE WHEN obs.cluster_id in ('COLCANSCREV_COD','COLCANSCR_COD')
              AND obs.mapped_concept_code != '368481000000103'
         THEN TRUE ELSE FALSE END AS is_completed_screening,
    CASE WHEN obs.cluster_id = 'COLCANSCRDEC_COD' THEN TRUE ELSE FALSE END AS is_declined_screening
    --no PCD refsets for either invitation non reposnse or unsuitable for bowel screening

FROM ({{ get_observations("'COLCANSCREV_COD', 'COLCANSCRDEC_COD', 'COLCANSCR_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC, ID