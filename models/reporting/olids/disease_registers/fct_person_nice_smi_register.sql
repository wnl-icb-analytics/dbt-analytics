{{ config(materialized='table', cluster_by=['person_id']) }}

WITH population AS (
    {{ nice_reference_population('current') }}
), smi AS (
    {{ calculate_smi_register() }}
), lithium AS (
    SELECT person_id, reporting_date
    FROM {{ ref('int_nice_ltc_population') }}
    WHERE is_on_lithium
)
SELECT population.person_id, population.reporting_date, population.age, population.practice_code,
    TRUE AS is_on_register,
    smi.person_id IS NOT NULL AS has_smi_diagnosis,
    lithium.person_id IS NOT NULL AS is_on_lithium,
    smi.latest_diagnosis_date, smi.latest_remission_date
FROM population
LEFT JOIN smi ON population.person_id = smi.person_id
    AND population.reporting_date = smi.reference_date
LEFT JOIN lithium ON population.person_id = lithium.person_id
    AND population.reporting_date = lithium.reporting_date
WHERE smi.person_id IS NOT NULL OR lithium.person_id IS NOT NULL
