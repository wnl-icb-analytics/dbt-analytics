{{ config(materialized='view') }}

-- NICE IND171: https://www.nice.org.uk/indicators/ind171
-- Referral to the NHS Diabetes Prevention Programme for adults newly diagnosed with non-diabetic hyperglycaemia in the preceding 12 months, excluding unresolved diabetes.
WITH indicator_population AS (
    SELECT
        ndh.person_id,
        age.age,
        ndh.earliest_diagnosis_date::DATE AS diagnosis_date
    FROM {{ ref('fct_person_ndh_register') }} AS ndh
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON ndh.person_id = age.person_id
    WHERE ndh.is_on_register
        AND ndh.earliest_diagnosis_date::DATE >= DATEADD(month, -12, CURRENT_DATE())
        AND age.age >= 18
        AND NOT COALESCE(ndh.has_diabetes_diagnosis AND NOT ndh.is_diabetes_resolved, FALSE)
),

referral AS (
    SELECT
        population.person_id,
        MIN(dpp.clinical_effective_date::DATE) AS first_referral_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_referral_ndpp_all') }} AS dpp
        ON population.person_id = dpp.person_id
        AND dpp.clinical_effective_date::DATE >= population.diagnosis_date
        AND LOWER(dpp.concept_display) NOT LIKE '%declin%'
    GROUP BY population.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.diagnosis_date,
        referral.first_referral_date AS latest_record_date,
        referral.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN referral
        ON population.person_id = referral.person_id
)

SELECT
    person_id,
    'IND171' AS indicator_id,
    'Diabetes: NDH diabetes prevention programme' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Non-diabetic hyperglycaemia diagnosed in the preceding 12 months (aged 18 and over)' AS condition_name,
    current_practice_code,
    current_practice_name,
    diagnosis_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
