{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

-- QOF v51 SMOK004 support: referral or pharmacotherapy, including PHARMDRUG observations and prescriptions.
WITH pharmacotherapy_codes AS (
    SELECT DISTINCT referenced_component_id::VARCHAR AS mapped_concept_code
    FROM {{ ref('stg_nhsd_snomed_sct_refset_simple') }}
    WHERE ref_set_id = 12465801000001106
        AND active
),

support_events AS (
    SELECT
        'OBSERVATION' AS source_kind,
        id::VARCHAR AS source_key,
        person_id,
        clinical_effective_date::DATE AS event_date
    FROM ({{ get_observations("'REFERSSSA_COD', 'PHARM_COD'", source='PCD') }}) AS observation

    UNION ALL

    SELECT
        'OBSERVATION' AS source_kind,
        observation.id::VARCHAR AS source_key,
        observation.person_id,
        -- Match the shared observation date correction, including the undated sentinel.
        CASE WHEN observation.clinical_effective_date > observation.date_recorded
            THEN observation.date_recorded
            ELSE COALESCE(observation.clinical_effective_date, '1900-01-01')
        END::DATE AS event_date
    FROM {{ ref('stg_olids_observation') }} AS observation
    INNER JOIN pharmacotherapy_codes AS codes
        ON observation.mapped_concept_code = codes.mapped_concept_code

    UNION ALL

    SELECT
        'MEDICATION_ORDER' AS source_kind,
        medication.id::VARCHAR AS source_key,
        person.person_id,
        medication.clinical_effective_date::DATE AS event_date
    FROM {{ ref('stg_olids_medication_order') }} AS medication
    INNER JOIN {{ ref('int_patient_person_unique') }} AS person
        ON medication.patient_id = person.patient_id
    INNER JOIN pharmacotherapy_codes AS codes
        ON medication.mapped_concept_code = codes.mapped_concept_code
)

SELECT DISTINCT
    source_kind,
    source_key,
    person_id,
    event_date
FROM support_events
WHERE event_date <= CURRENT_DATE()
