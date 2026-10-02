{% macro nice_ind115(reference='current') %}
-- NICE IND115: any home or ambulatory BP record in three months before register entry.
WITH population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        hypertension.earliest_diagnosis_date::DATE AS diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('HTN', reference) }}) AS hypertension
        ON population.person_id = hypertension.person_id
        AND population.reporting_date = hypertension.reporting_date
    WHERE hypertension.earliest_diagnosis_date::DATE BETWEEN '2014-04-01'::DATE AND population.reporting_date
),

selected AS (
    SELECT population.person_id, population.reporting_date,
        MAX(evidence.clinical_effective_date::DATE) AS latest_home_ambulatory_bp_date
    FROM population
    LEFT JOIN {{ ref('int_home_ambulatory_blood_pressure_all') }} AS evidence
        ON population.person_id = evidence.person_id
        AND evidence.clinical_effective_date::DATE BETWEEN DATEADD(month, -3, population.diagnosis_date)
            AND LEAST(population.diagnosis_date, population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
)

SELECT
    population.person_id,
    'IND115' AS indicator_id,
    'Hypertension: confirming diagnosis with HBPM or ABPM' AS indicator_name,
    population.reporting_date,
    '2014-04-01'::DATE AS measurement_period_start,
    population.age,
    'Hypertension diagnosed on or after 1 April 2014' AS condition_name,
    {{ nice_practice_columns('population', reference) }},
    population.diagnosis_date,
    selected.latest_home_ambulatory_bp_date,
    TRUE AS is_in_denominator,
    selected.latest_home_ambulatory_bp_date IS NOT NULL AS is_in_numerator,
    IFF(selected.latest_home_ambulatory_bp_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM population
LEFT JOIN selected
    ON population.person_id = selected.person_id
    AND population.reporting_date = selected.reporting_date
{% endmacro %}
