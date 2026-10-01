{{ config(materialized='view') }}

-- NICE IND223: https://www.nice.org.uk/indicators/ind223
-- Cancer care review within 12 months of the latest new cancer diagnosis for people diagnosed in the preceding 24 months; the register excludes non-melanoma skin cancer.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE profile.has_cancer AND profile.latest_cancer_diagnosis_date >= DATEADD(month, -24, CURRENT_DATE())
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_cancer_diagnosis_date AS diagnosis_date,
        population.first_cancer_care_review_after_diagnosis_date AS latest_review_date,
        CASE WHEN population.first_cancer_care_review_after_diagnosis_date <= DATEADD(month, 12, population.latest_cancer_diagnosis_date)
            THEN population.first_cancer_care_review_after_diagnosis_date END AS latest_record_date,
        COALESCE(population.first_cancer_care_review_after_diagnosis_date
            <= DATEADD(month, 12, population.latest_cancer_diagnosis_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND223' AS indicator_id,
    'Cancer: review within 12 months' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -24, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Cancer diagnosed in the preceding 24 months' AS condition_name,
    current_practice_code,
    current_practice_name,
    diagnosis_date,
    latest_review_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
