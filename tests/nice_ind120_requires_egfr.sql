-- NICE replaced serum creatinine with eGFR in February 2026.
SELECT indicator.person_id
FROM {{ ref('fct_person_diabetes_care_processes_ind120') }} AS indicator
WHERE indicator.is_in_numerator
    AND NOT EXISTS (
        SELECT 1
        FROM {{ ref('int_egfr_test_all') }} AS egfr
        WHERE egfr.person_id = indicator.person_id
            AND egfr.clinical_effective_date::DATE
                BETWEEN indicator.measurement_period_start AND indicator.reporting_date
    )
