{% macro nice_ind181(reference='current') %}
{#- Calculate IND181 at eligible person/reporting-date grain in current or by_month mode. -#}
-- NICE IND181: https://www.nice.org.uk/indicators/ind181
-- CVD risk assessment recorded in 3 years for people aged 25 to 84 with type 2 diabetes, no moderate or severe frailty and no statin order in 6 months; excludes CVD, FH and CKD.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.reporting_date,
        population.practice_code,
        population.practice_name,
        population.age,
        profile.latest_risk_score,
        profile.latest_risk_score_date,
        profile.latest_risk_assessment_date
    FROM {{ nice_ref('int_cvd_risk_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE population.age BETWEEN 25 AND 84
        AND profile.has_type2_diabetes
        AND COALESCE(profile.latest_frailty_severity, 'None') NOT IN ('Moderate', 'Severe')
        AND NOT COALESCE(profile.latest_statin_order_date >= DATEADD(month, -6, profile.reporting_date), FALSE)
        AND NOT profile.has_cvd_including_haemorrhagic_stroke
        AND NOT profile.has_familial_hypercholesterolaemia
        AND NOT profile.has_ckd
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_risk_score,
        population.latest_risk_score_date,
        population.latest_risk_assessment_date,
        COALESCE(
            population.latest_risk_assessment_date >= DATEADD(month, -36, population.reporting_date),
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND181' AS indicator_id,
    'Diabetes: CVD risk assessment' AS indicator_name,
    reporting_date,
    DATEADD(month, -36, reporting_date) AS measurement_period_start,
    age,
    'Type 2 diabetes not on a statin' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_risk_score,
    latest_risk_score_date,
    latest_risk_assessment_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
