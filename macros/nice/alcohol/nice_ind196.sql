{% macro nice_ind196(reference='current') %}
{#-
    Calculate NICE IND196 at each reference date.
    Args: reference is current or by_month.
    Returns: the IND196 detail columns, one eligible person per reporting_date.
-#}
-- NICE IND196: https://www.nice.org.uk/indicators/ind196
-- FAST or AUDIT-C screen within 3 months either side of a first hypertension diagnosis in the preceding 12 months; excludes alcohol-related disorders.
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
        profile.earliest_hypertension_date AS new_diagnosis_date,
        TRUE AS is_in_denominator
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_alcohol_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE profile.earliest_hypertension_date BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date
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
    'IND196' AS indicator_id,
    'Alcohol use: risk assessment for people with hypertension' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Newly diagnosed hypertension' AS condition_name,
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
