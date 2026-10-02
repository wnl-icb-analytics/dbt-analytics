{{ config(materialized='view') }}

-- NICE IND273: https://www.nice.org.uk/indicators/ind273
-- Asthma review in 12 months with a same-day written plan and an exacerbation count in the preceding month, for people aged 5 and over.
WITH asthma_diagnoses AS (
    SELECT person_id
    FROM {{ ref('int_asthma_diagnoses_all') }}
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
    GROUP BY person_id
    HAVING MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date::DATE END) IS NOT NULL
        AND (MAX(CASE WHEN is_resolved_code THEN clinical_effective_date::DATE END) IS NULL
            OR MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date::DATE END)
                > MAX(CASE WHEN is_resolved_code THEN clinical_effective_date::DATE END))
),

indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    INNER JOIN asthma_diagnoses ON profile.person_id = asthma_diagnoses.person_id
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE age.age >= 5
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_asthma_review_date AS latest_review_date,
        CASE WHEN population.latest_complete_asthma_review_date >= DATEADD(month, -12, CURRENT_DATE()) THEN population.latest_complete_asthma_review_date END AS latest_record_date,
        COALESCE(population.latest_complete_asthma_review_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND273' AS indicator_id,
    'Asthma: annual review' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Asthma (aged 5 and over)' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_review_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
