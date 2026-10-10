{% macro nice_ind116(reference='current') %}
{#-
    Calculate NICE IND116 for eligible women at each reference date.
    Args: reference is current or by_month.
    Returns: the IND116 detail columns, one person per reporting_date.
-#}
-- NICE IND116: https://www.nice.org.uk/indicators/ind116
WITH population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        evidence.latest_advice_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_contraception_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    INNER JOIN ({{ nice_register('DM', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    WHERE population.gender = 'Female'
        AND population.age >= 17 AND population.age < 45
), assessed AS (
    SELECT population.*,
        IFF(latest_advice_date >= DATEADD(month, -12, reporting_date), latest_advice_date, NULL) AS latest_record_date
    FROM population
)
SELECT person_id, 'IND116' AS indicator_id,
    'Contraception: advice for people with diabetes' AS indicator_name,
    'The percentage of women with diabetes aged 17 or over and who have not attained the age of 45 who have a record of being given information and advice about pregnancy or conception or contraception tailored to their pregnancy and contraceptive intentions recorded in the preceding 12 months.' AS indicator_description,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Women with diabetes, aged 17 to 44' AS denominator_description,
    {{ nice_practice_columns('result', reference) }},
    latest_record_date, TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
{% endmacro %}
