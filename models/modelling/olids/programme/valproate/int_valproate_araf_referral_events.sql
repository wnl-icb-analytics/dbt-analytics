{{ config(
    materialized='table',
    description='ARAF referral-coded source records from the expanded OLIDS observation feed, one row per person and conformed observation ID.') }}

-- The expanded observation feed already includes referral requests.
WITH referral_coded_events AS (
    SELECT
        o.patient_id,
        o.clinical_effective_date,
        o.id AS observation_id,
        o.source_entity,
        o.source_record_id,
        -- Preserve the IDs previously read directly from referral_request.
        CASE WHEN o.source_entity = 'referral_request' THEN o.source_record_id
            ELSE o.id END AS legacy_event_id,
        o.mapped_concept_code,
        o.mapped_concept_display
    FROM {{ ref('stg_olids_observation') }} AS o
)

SELECT
    pp.person_id,
    e.clinical_effective_date AS araf_referral_event_date,
    e.legacy_event_id AS araf_referral_id,
    e.observation_id AS araf_referral_observation_id,
    e.source_entity,
    e.source_record_id,
    e.mapped_concept_code AS araf_referral_concept_code,
    e.mapped_concept_display AS araf_referral_concept_display,
    vpc.code_category AS araf_referral_code_category
FROM referral_coded_events AS e
INNER JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vpc
    ON e.mapped_concept_code = vpc.code
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON e.patient_id = pp.patient_id
WHERE vpc.code_category = 'REFERRAL'
