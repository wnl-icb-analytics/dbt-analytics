{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

SELECT spine.person_id, dates.reporting_date, spine.age, spine.birth_date_approx,
    spine.gender, spine.practice_code, spine.practice_name,
    history.registration_start_date::DATE AS registration_start_date
FROM {{ ref('int_segmentation_person_month_spine') }} spine
INNER JOIN ({{ nice_reference_dates('by_month') }}) dates
    ON spine.month_end_date = dates.reporting_date
INNER JOIN {{ ref('dim_person_demographics') }} demographics
    ON spine.person_id = demographics.person_id
LEFT JOIN {{ ref('dim_person_demographics_historical') }} AS history
    ON spine.person_id = history.person_id
    AND dates.reporting_date >= history.effective_start_date
    AND (history.effective_end_date IS NULL OR dates.reporting_date < history.effective_end_date)
WHERE spine.is_active AND NOT COALESCE(demographics.is_test_patient, FALSE)
