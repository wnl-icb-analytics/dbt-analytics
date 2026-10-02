{% macro nice_ind319(reference='current') %}
WITH assessed AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        profile.bmi_date AS latest_bmi_date, profile.bmi_value,
        profile.is_recorded_white, profile.latest_weight_advice_date,
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
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Overweight, aged 18 to 39' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_bmi_date, bmi_value, is_recorded_white, bmi_lower_threshold, bmi_upper_threshold,
    latest_weight_advice_date, latest_weight_advice_date AS latest_record_date,
    TRUE AS is_in_denominator, latest_weight_advice_date IS NOT NULL AS is_in_numerator,
    IFF(latest_weight_advice_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
