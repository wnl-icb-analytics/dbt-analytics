{{ config(materialized='view') }}

-- NICE IND207: https://www.nice.org.uk/indicators/ind207
-- Structured medication review in 12 months for people with moderate or severe coded frailty or conditions in four or more
-- NICE IND205 clusters.
WITH indicator_population AS (
    SELECT
        profile.*,
        age.age
    FROM {{ ref('int_ltc_review_profile') }} AS profile
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON profile.person_id = age.person_id
    WHERE (
            profile.latest_frailty_severity IN ('Moderate', 'Severe')
            OR profile.multimorbidity_cluster_count >= 4
        )
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.ltc_count,
        population.multimorbidity_cluster_count,
        population.latest_frailty_severity,
        CASE WHEN COALESCE(population.latest_structured_medication_review_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE)
            THEN population.latest_structured_medication_review_date END AS latest_record_date,
        COALESCE(population.latest_structured_medication_review_date >= DATEADD(month, -12, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND207' AS indicator_id,
    'Multiple long-term conditions: medication review' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Moderate or severe frailty, or two or more long-term conditions' AS condition_name,
    current_practice_code,
    current_practice_name,
    ltc_count,
    multimorbidity_cluster_count,
    latest_frailty_severity,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
