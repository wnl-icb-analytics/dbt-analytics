{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All cancer diagnosis observations from clinical records.
Uses QOF cancer cluster IDs:
- CAN_COD: Cancer diagnoses

Clinical Purpose:
- QOF cancer register data collection
- Cancer care pathway monitoring
- Oncology treatment tracking
- Resolution/remission status tracking

QOF Context:
Cancer register includes persons with cancer diagnosis codes who have not
been resolved/in remission. Resolution logic applied in downstream fact models.
No specific age restrictions for cancer register.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per cancer observation.
Use this model as input for fct_person_cancer_register.sql which applies QOF business rules.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.episodicity_source_concept_id,
    ecm.target_code AS episodicity_code,
    ecm.target_display AS episodicity_display,

    -- Cancer-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'CAN_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,

    -- QOF: CAN_DAT is "latest first or new episode" — exclude reviews/ended
    CASE
        WHEN ecm.source_display IN ('Review', 'Ended', 'Changed', 'Evolved', 'Flare Up') THEN FALSE
        ELSE TRUE
    END AS is_first_or_new_episode,

    -- Cancer observation type determination
    CASE
        WHEN obs.cluster_id = 'CAN_COD' THEN 'Cancer Diagnosis'
        ELSE 'Unknown'
    END AS cancer_observation_type,
    trud.icd10_code

FROM ({{ get_observations("'CAN_COD'", source='PCD') }}) obs
LEFT JOIN {{ ref('stg_olids_enriched_concept_map') }} ecm
    ON obs.episodicity_source_concept_id = ecm.source_concept_id

-- Addition 2028-08-14: Mapping the SNOMED concept code to ICD10 and use the overwrite table to determine which map to use
-- Gender is required for some mappings
LEFT JOIN {{ ref('dim_person_demographics')}} dem
    ON obs.person_id = dem.person_id

-- Pull in the overwrite map
LEFT JOIN {{ ref('cancer_snomed_code_to_icd10_override')}} ow
    ON obs.mapped_concept_code = ow.concept_code
    AND case 
            when ow.gender is not null 
                then ow.gender = dem.gender
            else true
        end

-- Map to ICD10
LEFT JOIN {{ ref('stg_reference_snomed_to_icd10_latest')}} trud
    ON obs.mapped_concept_code = trud.snomed_concept_id
    AND trud.map_priority = (
            CASE
                WHEN ow.concept_code IS NOT NULL
                THEN ow.map_priority
                ELSE 1
            END 
        )
    AND trud.map_block = 1
    AND trud.map_group = 1

ORDER BY person_id, clinical_effective_date, id