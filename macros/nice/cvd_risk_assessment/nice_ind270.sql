{% macro nice_ind270(reference='current') %}
{#- Calculate IND270 at eligible person/reporting-date grain in current or by_month mode. -#}
-- NICE IND270: https://www.nice.org.uk/indicators/ind270
-- CVD risk assessment recorded in 3 years for people aged 43 to 84 who smoke, have obesity, hypertension or a latest total cholesterol above 5 mmol/L; same exclusions as IND269.
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
    WHERE population.age BETWEEN 43 AND 84
        AND (
            profile.is_current_smoker
            OR profile.has_obesity
            OR profile.has_hypertension
            OR profile.latest_total_cholesterol > 5
        )
        AND NOT profile.has_type1_diabetes
        AND NOT profile.has_cvd
        AND NOT profile.has_familial_hypercholesterolaemia
        AND NOT profile.has_ckd
        AND NOT COALESCE(profile.latest_lipid_lowering_order_date >= DATEADD(month, -6, profile.reporting_date), FALSE)
        AND NOT COALESCE(profile.max_risk_score_ever >= 20, FALSE)
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
        -- NICE counts a recorded risk score, so scored QRISK results only
        COALESCE(
            population.latest_risk_score_date >= DATEADD(month, -36, population.reporting_date),
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND270' AS indicator_id,
    'Cardiovascular disease prevention: risk assessment (modifiable risk factors)' AS indicator_name,
    reporting_date,
    DATEADD(month, -36, reporting_date) AS measurement_period_start,
    age,
    'Aged 43 to 84 with a modifiable risk factor' AS condition_name,
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
