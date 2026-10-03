{% macro nice_ind117(reference='current') %}
{#-
    Calculate NICE IND117 for eligible women at each reference date.
    Args: reference is current or by_month.
    Returns: the IND117 detail columns, one person per reporting_date.
-#}
-- NICE IND117: https://www.nice.org.uk/indicators/ind117
WITH population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        evidence.latest_advice_date, evidence.latest_asm_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_contraception_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    INNER JOIN ({{ nice_register('EP', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    WHERE population.gender = 'Female'
        AND population.age >= 18 AND population.age < 45
        AND (evidence.first_sterilisation_hysterectomy_date IS NULL
            OR evidence.first_sterilisation_hysterectomy_date > population.reporting_date)
), assessed AS (
    SELECT population.*,
        IFF(latest_advice_date >= DATEADD(month, -12, reporting_date), latest_advice_date, NULL) AS latest_record_date
    FROM population
)
SELECT person_id, 'IND117' AS indicator_id,
    'Contraception advice for women with epilepsy' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Women aged 18 to 44 on the epilepsy register' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_asm_date,
    latest_record_date, TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
{% endmacro %}
