{{ config(materialized='table', cluster_by=['person_id']) }}

WITH population AS (
    {{ nice_reference_population('current') }}
), frailty AS (
    {{ calculate_frailty_register() }}
)
SELECT population.person_id, population.reporting_date, population.age, population.practice_code,
    TRUE AS is_on_register, frailty.latest_frailty_severity, frailty.latest_diagnosis_date
FROM population
INNER JOIN frailty ON population.person_id = frailty.person_id
    AND population.reporting_date = frailty.reference_date
WHERE population.age >= 65 AND frailty.latest_frailty_severity IN ('Moderate', 'Severe')
