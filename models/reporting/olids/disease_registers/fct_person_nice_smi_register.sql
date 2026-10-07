{{ config(materialized='table', cluster_by=['person_id']) }}

WITH population AS (
    {{ nice_reference_population('current') }}
), reference_dates AS (
    SELECT DISTINCT reporting_date AS reference_date FROM population
), smi AS (
    {{ calculate_smi_register() }}
), lithium AS (
    {{ calculate_nice_lithium_therapy('reference_dates') }}
)
SELECT population.person_id, population.reporting_date, population.age, population.practice_code,
    TRUE AS is_on_register,
    smi.person_id IS NOT NULL AS has_smi_diagnosis,
    lithium.person_id IS NOT NULL AS is_on_lithium,
    smi.latest_diagnosis_date, smi.latest_remission_date, lithium.latest_lithium_order_date
FROM population
LEFT JOIN smi ON population.person_id = smi.person_id
    AND population.reporting_date = smi.reference_date
LEFT JOIN lithium ON population.person_id = lithium.person_id
    AND population.reporting_date = lithium.reporting_date
WHERE smi.person_id IS NOT NULL OR lithium.person_id IS NOT NULL
