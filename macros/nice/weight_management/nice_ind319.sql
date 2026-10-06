{#-
    Calculate NICE IND319 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND319 detail columns, one person per reporting_date.
-#}
{% macro nice_ind319(reference='current') %}
-- NICE IND319: https://www.nice.org.uk/indicators/ind319
-- Weight advice within 90 days of a qualifying measured BMI for people aged 18 to 39, using ethnicity-specific overweight thresholds.
WITH assessed AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        profile.bmi_date AS latest_bmi_date, profile.bmi_value,
        profile.is_recorded_white, profile.latest_weight_advice_date, profile.earliest_qualifying_bmi_date,
        IFF(profile.is_recorded_white, 25.0, 23.0) AS bmi_lower_threshold,
        IFF(profile.is_recorded_white, 29.9, 27.4) AS bmi_upper_threshold
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_weight_profile', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    WHERE population.age BETWEEN 18 AND 39
        AND profile.bmi_date BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date
        AND profile.bmi_value BETWEEN IFF(profile.is_recorded_white, 25.0, 23.0)
            AND IFF(profile.is_recorded_white, 29.9, 27.4)
)
SELECT person_id, 'IND319' AS indicator_id,
    'Weight management: advice for people living with overweight (18 to 39 years)' AS indicator_name,
    'The percentage of patients aged 18 to 39 years with a BMI of 23 kg/m^2 to 27.4 kg/m^2 (or 25 kg/m^2 to 29.9 kg/m^2 if ethnicity is recorded as White) in the preceding 12 months who have been given weight management advice within 90 days of the BMI being recorded.' AS indicator_description,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Overweight, aged 18 to 39' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_bmi_date, bmi_value, is_recorded_white, bmi_lower_threshold, bmi_upper_threshold,
    latest_weight_advice_date, latest_weight_advice_date AS latest_record_date,
    TRUE AS is_in_denominator, latest_weight_advice_date IS NOT NULL AS is_in_numerator,
    IFF(latest_weight_advice_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
WHERE DATEADD(day, 90, earliest_qualifying_bmi_date) <= reporting_date
{% endmacro %}
