{% macro nice_ind202(reference='current') %}
{#-
    Calculate NICE IND202 at each reference date.
    Args: reference is current or by_month.
    Returns: the IND202 detail columns, one eligible person per reporting_date.
-#}
-- NICE IND202: https://www.nice.org.uk/indicators/ind202
-- Brief intervention within 3 months of any qualifying positive screen for people with a listed LTC and a positive screen in the preceding 2 years; excludes alcohol-related disorders.
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
        TRUE AS is_in_denominator
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_alcohol_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE (profile.has_chd OR profile.has_atrial_fibrillation OR profile.has_heart_failure
            OR profile.has_stroke_tia OR profile.has_diabetes OR profile.has_dementia)
        AND evidence.latest_positive_alcohol_screen_date BETWEEN DATEADD(month, -24, population.reporting_date) AND population.reporting_date
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
        AND pair.screen_date >= DATEADD(month, -24, population.reporting_date)
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
    'IND202' AS indicator_id,
    'Alcohol use: brief intervention for people with a long-term condition' AS indicator_name,
    reporting_date,
    DATEADD(month, -24, reporting_date) AS measurement_period_start,
    age,
    'Long-term condition with a positive alcohol screen' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
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
            AND screen.screen_date >= DATEADD(month, -24, assessed.reporting_date)
            AND DATEADD(month, 3, screen.screen_date) <= assessed.reporting_date
    )
{% endmacro %}
