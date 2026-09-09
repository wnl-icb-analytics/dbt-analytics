{{ config(
    materialized='table',
    description='Intermediate table extracting all ARAF referral-related events for each person, using mapped concepts, observation, and valproate program codes (category REFERRAL).') }}

-- the new feed classifies coded referrals into referral_request where the
-- legacy feed recorded them as observations; read both so the model is
-- portable across feeds
WITH referral_coded_events AS (
    SELECT
        o.patient_id,
        o.clinical_effective_date,
        o.id,
        o.mapped_concept_code,
        o.mapped_concept_display
    FROM {{ ref('stg_olids_observation') }} AS o

    UNION ALL

    SELECT
        r.patient_id,
        r.clinical_effective_date,
        r.id,
        r.mapped_concept_code,
        r.mapped_concept_display
    FROM {{ ref('stg_olids_referral_request') }} AS r
)

SELECT
    pp.person_id,
    e.clinical_effective_date AS araf_referral_event_date,
    e.id AS araf_referral_ID,
    e.mapped_concept_code AS araf_referral_concept_code,
    e.mapped_concept_display AS araf_referral_concept_display,
    vpc.code_category AS araf_referral_code_category
FROM referral_coded_events AS e
INNER JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vpc
    ON e.mapped_concept_code = vpc.code
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON e.patient_id = pp.patient_id
WHERE vpc.code_category = 'REFERRAL'
