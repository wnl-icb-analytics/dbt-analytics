{{ config(
    materialized='table',
    description="Neurology records in OLIDS, one row per observation linked to a person. Uses the valproate code list's NEUROLOGY category. Includes inactive and deceased people of any age or sex, whether or not they take valproate. Event dates are unrestricted; the dashboard applies its population rules separately.") }}

SELECT
    pp.person_id,
    o.clinical_effective_date AS neurology_event_date,
    o.id AS neurology_ID,
    o.mapped_concept_code AS neurology_concept_code,
    o.mapped_concept_display AS neurology_concept_display,
    vpc.code_category AS neurology_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vpc
    ON o.mapped_concept_code = vpc.code
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'NEUROLOGY'
