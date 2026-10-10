{{ config(tags=['monthly-full', 'nice-history']) }}

SELECT COUNT(*) AS failure_count
FROM {{ ref('fct_person_nice_indicator_status_by_month') }} AS status
LEFT JOIN {{ ref('int_nice_reference_population_by_month') }} AS population
    ON status.person_id = population.person_id
    AND status.reporting_date = population.reporting_date
WHERE population.person_id IS NULL
    OR status.person_id IS NULL
    OR status.indicator_id IS NULL
    OR NOT COALESCE(status.is_in_denominator, FALSE)
    OR status.is_in_numerator IS NULL
    OR status.indicator_status IS NULL
    OR (status.indicator_status = 'ACHIEVED') <> status.is_in_numerator
HAVING COUNT(*) > 0
