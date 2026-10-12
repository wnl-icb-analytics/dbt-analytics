{{ config(materialized='table', cluster_by=['person_id']) }}

WITH population AS (
    {{ nice_reference_population('current') }}
), af AS (
    {{ calculate_atrial_fibrillation_register() }}
)
SELECT population.person_id, population.reporting_date, population.age, population.practice_code,
    TRUE AS is_on_register,
    NOT af.is_on_register AS is_resolved,
    af.earliest_diagnosis_date, af.latest_diagnosis_date, af.latest_resolved_date
FROM population
INNER JOIN af ON population.person_id = af.person_id
    AND population.reporting_date = af.reference_date
WHERE af.latest_diagnosis_date IS NOT NULL
