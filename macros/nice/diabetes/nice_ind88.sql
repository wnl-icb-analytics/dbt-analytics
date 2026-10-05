{% macro nice_ind88(reference='current') %}
{#-
    Calculate NICE IND88 at each reference date using its reviewed rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND88: https://www.nice.org.uk/indicators/ind88
-- Referral to a structured education programme within 9 months of joining the diabetes register, for people diagnosed in the completed follow-up cohort.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        register.earliest_diagnosis_date::DATE AS diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('DM', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    WHERE register.earliest_diagnosis_date::DATE > DATEADD(month, -21, population.reporting_date)
        AND register.earliest_diagnosis_date::DATE <= DATEADD(month, -9, population.reporting_date)
),

first_referral AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MIN(event.clinical_effective_date::DATE) AS latest_record_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_diabetes_structured_education_all') }} AS event
        ON population.person_id = event.person_id
        AND event.clinical_effective_date::DATE
            BETWEEN population.diagnosis_date AND LEAST(DATEADD(month, 9, population.diagnosis_date), population.reporting_date)
        -- A referral counts; attendance or an offer alone does not.
        AND event.record_type = 'REFERRED'
    GROUP BY population.person_id, population.reporting_date
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        population.diagnosis_date,
        referral.latest_record_date,
        referral.person_id IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN first_referral AS referral
        ON population.person_id = referral.person_id
        AND population.reporting_date = referral.reporting_date
)

SELECT
    person_id,
    'IND88' AS indicator_id,
    'Diabetes: referral for structured education' AS indicator_name,
    reporting_date,
    DATEADD(month, -21, reporting_date) AS measurement_period_start,
    age,
    'Diabetes diagnosed 9 to 21 months ago'::VARCHAR(45) AS condition_name,
    {{ nice_practice_columns(none, reference) }},
    diagnosis_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
