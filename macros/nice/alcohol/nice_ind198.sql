{% macro nice_ind198(reference='current') %}
{#-
    Calculate NICE IND198 at each reference date.
    Args: reference is current or by_month.
    Returns: the IND198 detail columns, one eligible person per reporting_date.
-#}
-- NICE IND198: https://www.nice.org.uk/indicators/ind198
-- FAST or AUDIT-C screen within 3 months either side of a first depression or anxiety diagnosis in the preceding 12 months, aged 10 and over; excludes alcohol-related disorders.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        evidence.latest_alcohol_screen_date,
        evidence.latest_alcohol_screen_tool,
        evidence.latest_alcohol_screen_score,
        evidence.latest_positive_alcohol_screen_date,
        evidence.latest_intervention_after_positive_screen_date,
        profile.earliest_depression_anxiety_date AS new_diagnosis_date,
        TRUE AS is_in_denominator
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_alcohol_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE profile.earliest_depression_anxiety_date > DATEADD(month, -15, population.reporting_date)
        AND profile.earliest_depression_anxiety_date <= DATEADD(month, -3, population.reporting_date)
        AND population.age >= 10
        AND NOT profile.has_nice_alcohol_disorder
),

qualifying_screens AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MAX(screen.clinical_effective_date::DATE) AS latest_record_date
    FROM indicator_population AS population
    LEFT JOIN {{ ref('int_alcohol_screening_all') }} AS screen
        ON population.person_id = screen.person_id
        AND screen.screening_tool IN ('FAST', 'AUDIT-C')
        AND screen.clinical_effective_date::DATE BETWEEN DATEADD(month, -3, population.new_diagnosis_date)
            AND LEAST(DATEADD(month, 3, population.new_diagnosis_date), population.reporting_date)
    GROUP BY population.person_id, population.reporting_date
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.new_diagnosis_date,
        population.latest_alcohol_screen_date,
        population.latest_alcohol_screen_tool,
        population.latest_alcohol_screen_score,
        population.latest_positive_alcohol_screen_date,
        population.latest_intervention_after_positive_screen_date,
        qualifying.latest_record_date,
        qualifying.latest_record_date IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN qualifying_screens AS qualifying
        ON population.person_id = qualifying.person_id
        AND population.reporting_date = qualifying.reporting_date
)

SELECT
    person_id,
    'IND198' AS indicator_id,
    'Alcohol use: risk assessment for people with depression or anxiety' AS indicator_name,
    reporting_date,
    DATEADD(month, -15, reporting_date) AS measurement_period_start,
    age,
    'New depression or anxiety, aged 10 or over' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    new_diagnosis_date,
    latest_alcohol_screen_date,
    latest_alcohol_screen_tool,
    latest_alcohol_screen_score,
    latest_positive_alcohol_screen_date,
    latest_intervention_after_positive_screen_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
