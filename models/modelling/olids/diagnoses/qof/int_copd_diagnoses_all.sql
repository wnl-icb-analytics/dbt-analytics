{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All COPD disorder, administrative and resolved observations from clinical records.
Uses QOF v51 COPD cluster IDs:
- COPDDIAG_COD: codes indicating the presence of COPD disorder
- COPDPROC_COD: administrative codes indicating the presence of COPD disorder
- COPDRES_COD: COPD resolved codes

Clinical Purpose:
- QOF COPD register data collection
- COPD spirometry confirmation requirements (post-April 2023)
- Respiratory management monitoring
- Resolution status tracking

The QOF register applies a two-year window to administrative codes downstream.
This observation-level model retains all history so clinical episode consumers can
use older administrative evidence without inheriting the register restriction.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per COPD observation.
Use this model as input for fct_person_copd_register.sql which applies QOF business rules and spirometry requirements.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- COPD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'COPDDIAG_COD' THEN TRUE ELSE FALSE END AS is_disorder_code,
    CASE WHEN obs.cluster_id = 'COPDPROC_COD' THEN TRUE ELSE FALSE END AS is_admin_code,
    CASE WHEN obs.cluster_id = 'COPDRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,

    -- COPD observation type determination
    CASE
        WHEN obs.cluster_id = 'COPDDIAG_COD' THEN 'COPD Disorder'
        WHEN obs.cluster_id = 'COPDPROC_COD' THEN 'COPD Administrative'
        WHEN obs.cluster_id = 'COPDRES_COD' THEN 'COPD Resolved'
        ELSE 'Unknown'
    END AS copd_observation_type

FROM ({{ get_observations("'COPDDIAG_COD', 'COPDPROC_COD', 'COPDRES_COD'", source='PCD') }}) obs

ORDER BY person_id, clinical_effective_date, id
