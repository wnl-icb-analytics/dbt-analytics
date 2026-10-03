{% macro nice_ind148(reference='current') %}
{#-
    Calculate NICE IND148 for eligible women at each reference date.
    Args: reference is current or by_month.
    Returns: the IND148 detail columns, one person per reporting_date.
-#}
-- NICE IND148: https://www.nice.org.uk/indicators/ind148
WITH population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        evidence.latest_larc_date, evidence.latest_oral_patch_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_contraception_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE population.gender = 'Female'
        AND population.age <= 54
        AND evidence.latest_oral_patch_date > DATEADD(month, -12, population.reporting_date)
), assessed AS (
    SELECT population.*,
        IFF(latest_larc_date >= DATEADD(month, -12, reporting_date), latest_larc_date, NULL) AS latest_record_date
    FROM population
)
SELECT person_id, 'IND148' AS indicator_id,
    'LARC information for women using oral or patch contraception' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Women aged 54 or under using practice-issued oral or patch contraception' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_oral_patch_date,
    latest_record_date, TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
{% endmacro %}
