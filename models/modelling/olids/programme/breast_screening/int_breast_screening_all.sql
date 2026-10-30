{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
         tags=['screening_programme'],
        persist_docs={"relation": true})
}}

/*
All breast screening programme observations from clinical records.
Uses PCD REFSET breast screening cluster IDs:
- BRCANSCR_COD: Breast screening completed codes
- BRCANSCRDEC_COD: Breast screening declined codes

Clinical Purpose:
- Breast screening programme data collection
- Observation-level screening events tracking
- Foundation data for programme analysis

Key Business Rules:
- Females aged 50 to 71 : invited every 3 years
- Declined/non-response status: valid for 12 months only
- Unsuitable status: permanent unless superseded by completed screening

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per breast screening observation.
*/

SELECT
    obs.id,
    obs.date_recorded,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Screening type classification
    CASE
        WHEN obs.cluster_id  = 'BRCANSCR_COD' THEN 'Breast Screening Completed'
        WHEN obs.cluster_id = 'BRCANSCRDEC_COD' THEN 'Screening Declined'
        ELSE 'Unknown'
    END AS screening_observation_type,
    --no PCD refsets for either invitation non reposnse or unsuitable for bowel screening

    -- Simple screening type flags based on core cluster codes
    CASE WHEN obs.cluster_id = 'BRCANSCR_COD' THEN TRUE ELSE FALSE END AS is_completed_screening,
    CASE WHEN obs.cluster_id = 'BRCANSCRDEC_COD' THEN TRUE ELSE FALSE END AS is_declined_screening
    --no PCD refsets for either invitation non reposnse or unsuitable for breast screening

FROM ({{ get_observations("'BRCANSCR_COD', 'BRCANSCRDEC_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC, ID