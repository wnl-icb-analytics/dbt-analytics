{% macro nice_ind161(reference='current') %}
{#- Calculate IND161 at eligible person/reporting-date grain in current or by_month mode. -#}
-- NICE IND161: https://www.nice.org.uk/indicators/ind161
-- CVD risk assessment within 3 months either side of a first hypertension or type 2 diabetes diagnosis in the preceding 12 months, aged 25 to 84; excludes CVD, CKD, FH and type 1 diabetes.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.reporting_date,
        population.practice_code,
        population.practice_name,
        population.age,
        profile.latest_risk_score,
        profile.latest_risk_score_date,
        profile.latest_risk_assessment_date,
        LEAST_IGNORE_NULLS(
            CASE WHEN profile.earliest_hypertension_date > DATEADD(month, -15, profile.reporting_date)
                AND profile.earliest_hypertension_date <= DATEADD(month, -3, profile.reporting_date)
                THEN profile.earliest_hypertension_date END,
            CASE WHEN profile.earliest_type2_diabetes_date > DATEADD(month, -15, profile.reporting_date)
                AND profile.earliest_type2_diabetes_date <= DATEADD(month, -3, profile.reporting_date)
                THEN profile.earliest_type2_diabetes_date END
        ) AS new_diagnosis_date
    FROM {{ nice_ref('int_cvd_risk_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE population.age BETWEEN 25 AND 84
        AND (
            profile.earliest_hypertension_date > DATEADD(month, -15, profile.reporting_date)
                AND profile.earliest_hypertension_date <= DATEADD(month, -3, profile.reporting_date)
            OR profile.earliest_type2_diabetes_date > DATEADD(month, -15, profile.reporting_date)
                AND profile.earliest_type2_diabetes_date <= DATEADD(month, -3, profile.reporting_date)
        )
        AND NOT profile.has_cvd_including_haemorrhagic_stroke
        AND NOT profile.has_ckd
        AND NOT profile.has_familial_hypercholesterolaemia
        AND NOT profile.has_type1_diabetes
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
        population.new_diagnosis_date,
        EXISTS (
            SELECT 1
            FROM {{ ref('int_cvd_risk_assessment_all') }} AS assessment
            WHERE assessment.person_id = population.person_id
                AND assessment.clinical_effective_date::DATE
                    BETWEEN DATEADD(month, -3, population.new_diagnosis_date)
                    AND LEAST(DATEADD(month, 3, population.new_diagnosis_date), population.reporting_date)
        ) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND161' AS indicator_id,
    'Cardiovascular disease prevention: cardiovascular risk assessment for people newly diagnosed with hypertension or T2DM' AS indicator_name,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'New hypertension or type 2 diabetes, aged 25 to 84' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_risk_score,
    latest_risk_score_date,
    latest_risk_assessment_date,
    new_diagnosis_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
