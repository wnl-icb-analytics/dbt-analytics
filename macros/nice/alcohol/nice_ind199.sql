{% macro nice_ind199(reference='current') %}
{#-
    Calculate NICE IND199 at each reference date.
    Args: reference is current or by_month.
    Returns: the IND199 detail columns, one eligible person per reporting_date.
-#}
-- NICE IND199: https://www.nice.org.uk/indicators/ind199
-- Brief intervention within 3 months of any qualifying positive screen for people aged 10 and over with a first depression or anxiety diagnosis and a positive screen in the preceding 12 months; excludes alcohol-related disorders.
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
    WHERE profile.earliest_depression_anxiety_date BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date
        AND population.age >= 10
        AND evidence.latest_positive_alcohol_screen_date BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date
        AND NOT profile.has_nice_alcohol_disorder
),

qualifying_pairs AS (
    SELECT DISTINCT
        population.person_id,
        population.reporting_date,
        pair.screen_date
    FROM indicator_population AS population
    INNER JOIN {{ ref('int_nice_alcohol_screen_intervention') }} AS pair
        ON population.person_id = pair.person_id
        AND pair.screen_date <= population.reporting_date
        AND pair.screen_date >= DATEADD(month, -12, population.reporting_date)
        -- A later maximum must not hide a pair already completed by this month-end.
        AND pair.first_intervention_date <= population.reporting_date
),

qualifying_interventions AS (
    SELECT
        pair.person_id,
        pair.reporting_date,
        MAX(intervention.clinical_effective_date::DATE) AS latest_record_date
    FROM qualifying_pairs AS pair
    INNER JOIN {{ ref('int_alcohol_intervention') }} AS intervention
        ON pair.person_id = intervention.person_id
        AND intervention.alcohol_advice_services = 'Yes'
        AND intervention.clinical_effective_date::DATE BETWEEN pair.screen_date
            AND LEAST(DATEADD(month, 3, pair.screen_date), pair.reporting_date)
    GROUP BY pair.person_id, pair.reporting_date
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
    LEFT JOIN qualifying_interventions AS qualifying
        ON population.person_id = qualifying.person_id
        AND population.reporting_date = qualifying.reporting_date
)

SELECT
    person_id,
    'IND199' AS indicator_id,
    'Alcohol use: brief intervention for people with depression or anxiety' AS indicator_name,
    'The percentage of patients with a new diagnosis of depression or anxiety and a FAST score of 3 or more or AUDIT-C score of 5 or more in the preceding 12 months, who have received brief intervention to help them reduce their alcohol related risk within 3 months of the score being recorded.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'New depression or anxiety with a positive alcohol screen' AS denominator_description,
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
WHERE EXISTS (
        SELECT 1
        FROM {{ ref('int_nice_alcohol_screen_intervention') }} AS screen
        WHERE screen.person_id = assessed.person_id
            AND screen.screen_date >= DATEADD(month, -12, assessed.reporting_date)
            AND DATEADD(month, 3, screen.screen_date) <= assessed.reporting_date
    )
{% endmacro %}
