{{ config(
    materialized='table',
    description='Intermediate table extracting all ARAF referral-related events for each person, using mapped concepts, observation, and valproate program codes (category REFERRAL).') }}

-- The new feed classifies coded referrals into referral_request where the
-- legacy feed recorded them as observations. Read both so the model is
-- portable across feeds. The expanded observation feed also copies each
-- referral request with a different ID; keep the referral-request ID when
-- both rows are the same record, matching the IDs already held in
-- person-level arrays.
WITH valproate_referral_codes AS (
    SELECT
        code,
        code_category
    FROM {{ ref('stg_reference_valproate_prog_codes') }}
    WHERE code_category = 'REFERRAL'
),

referral_request_events AS (
    SELECT
        r.patient_id,
        r.clinical_effective_date,
        r.id,
        r.mapped_concept_code,
        r.mapped_concept_display,
        vpc.code_category,
        r.observation_id
    FROM {{ ref('stg_olids_referral_request') }} AS r
    INNER JOIN valproate_referral_codes AS vpc
        ON r.mapped_concept_code = vpc.code
),

observation_events AS (
    SELECT
        o.patient_id,
        o.clinical_effective_date,
        o.id,
        o.mapped_concept_code,
        o.mapped_concept_display,
        vpc.code_category
    FROM {{ ref('stg_olids_observation') }} AS o
    INNER JOIN valproate_referral_codes AS vpc
        ON o.mapped_concept_code = vpc.code
    WHERE NOT EXISTS (
        SELECT 1
        FROM referral_request_events AS r
        WHERE r.observation_id = o.id
    )
),

referral_coded_events AS (
    SELECT
        patient_id,
        clinical_effective_date,
        id,
        mapped_concept_code,
        mapped_concept_display,
        code_category
    FROM referral_request_events

    UNION ALL

    SELECT
        patient_id,
        clinical_effective_date,
        id,
        mapped_concept_code,
        mapped_concept_display,
        code_category
    FROM observation_events
)

SELECT
    pp.person_id,
    e.clinical_effective_date AS araf_referral_event_date,
    e.id AS araf_referral_ID,
    e.mapped_concept_code AS araf_referral_concept_code,
    e.mapped_concept_display AS araf_referral_concept_display,
    e.code_category AS araf_referral_code_category
FROM referral_coded_events AS e
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON e.patient_id = pp.patient_id
