{% macro nice_ind149(reference='current') %}
{#-
    Calculate NICE IND149 for eligible women at each reference date.
    Args: reference is current or by_month.
    Returns: the IND149 detail columns, one person per reporting_date.
-#}
-- NICE IND149: https://www.nice.org.uk/indicators/ind149
WITH population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        evidence.latest_ehc_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_contraception_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE population.gender = 'Female'
        AND population.age <= 54
        AND evidence.latest_ehc_date > DATEADD(month, -12, population.reporting_date)
), assessed AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name, population.latest_ehc_date,
        MAX(IFF(advice.advice_modality = 'NON_SPECIFIC', advice.event_date, NULL)) AS latest_non_specific_advice_date,
        MAX(IFF(advice.advice_modality = 'WRITTEN', advice.event_date, NULL)) AS latest_written_advice_date,
        MAX(IFF(advice.advice_modality = 'VERBAL', advice.event_date, NULL)) AS latest_verbal_advice_date,
        COALESCE(BOOLOR_AGG(advice.advice_modality = 'UNKNOWN'), FALSE) AS has_unknown_modality_advice
    FROM population
    LEFT JOIN {{ ref('int_nice_larc_advice_all') }} AS advice
        ON population.person_id = advice.person_id
        AND advice.event_date >= population.latest_ehc_date
        AND advice.event_date <= DATEADD(day, 31, population.latest_ehc_date)
        AND advice.event_date <= population.reporting_date
    GROUP BY population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name, population.latest_ehc_date
), result AS (
    SELECT assessed.*,
        GREATEST_IGNORE_NULLS(latest_non_specific_advice_date,
            IFF(latest_written_advice_date IS NOT NULL AND latest_verbal_advice_date IS NOT NULL,
                GREATEST(latest_written_advice_date, latest_verbal_advice_date), NULL)) AS latest_record_date
    FROM assessed
)
SELECT person_id, 'IND149' AS indicator_id,
    'LARC information after emergency hormonal contraception' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Women aged 54 or under using practice-issued emergency hormonal contraception' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_ehc_date,
    latest_non_specific_advice_date,
    latest_written_advice_date,
    latest_verbal_advice_date,
    has_unknown_modality_advice,
    latest_record_date, TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM result
{% endmacro %}
