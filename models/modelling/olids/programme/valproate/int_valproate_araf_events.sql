{{ config(
    materialized='table',
    description="Recorded annual risk acknowledgement form (ARAF) events. One row per observation matching the ARAF category of the valproate programme code list, with a person mapping. Each code's lookback limit applies where configured. There is no restriction to valproate users, sex, age, registration or death, and no upper event-date limit.") }}

SELECT
    pp.person_id,
    o.clinical_effective_date AS araf_event_date,
    o.id AS araf_ID,
    o.mapped_concept_code AS araf_concept_code,
    o.mapped_concept_display AS araf_concept_display,
    vpc.code_category AS araf_code_category,
    coalesce (vpc.code_category = 'ARAF', FALSE)
        AS is_specific_araf_form_code
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vpc
    ON o.mapped_concept_code = vpc.code
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE
    vpc.code_category = 'ARAF'
    AND (
        vpc.lookback_years_offset IS NULL
        OR o.clinical_effective_date
        >= dateadd(YEAR, vpc.lookback_years_offset, current_date())
    )
