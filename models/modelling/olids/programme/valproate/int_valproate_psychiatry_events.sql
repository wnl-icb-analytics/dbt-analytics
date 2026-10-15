{{ config(
    materialized='table',
    description="Recorded psychiatry events identified by the valproate programme code list's PSYCH category. The OLIDS population comes from NCL GP records. One row per matching observation with a person mapping. There is no restriction to valproate users, sex, age, registration, death or event date; dashboard population restrictions apply downstream.") }}

SELECT
    pp.person_id,
    o.clinical_effective_date AS psych_event_date,
    o.id AS psych_ID,
    o.mapped_concept_code AS psych_concept_code,
    o.mapped_concept_display AS psych_concept_display,
    vpc.code_category AS psych_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vpc
    ON o.mapped_concept_code = vpc.code
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'PSYCH'
