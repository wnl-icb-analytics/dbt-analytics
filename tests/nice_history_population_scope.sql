{{ config(tags=['monthly-full', 'nice-history']) }}

SELECT COUNT(*) AS failure_count
FROM {{ ref('int_nice_reference_population_by_month') }} population
INNER JOIN {{ ref('dim_person_demographics') }} demographics USING (person_id)
LEFT JOIN {{ ref('int_segmentation_person_month_spine') }} spine
    ON population.person_id = spine.person_id AND population.reporting_date = spine.month_end_date
WHERE COALESCE(demographics.is_test_patient, FALSE)
    OR NOT COALESCE(spine.is_active, FALSE)
HAVING COUNT(*) > 0
