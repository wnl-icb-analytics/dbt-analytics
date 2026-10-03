{% macro calculate_nice_bmi_register(register_type, reference='current', reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
{#-
    Calculate NICE IND237 or IND238 membership from the latest measured adult BMI.
    Args: register_type is overweight or obesity; reference_dates supplies reference_date rows.
    Returns: one register member per person and reference_date, with BMI and threshold evidence.
-#}
{% if register_type not in ['overweight', 'obesity'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE BMI register: ' ~ register_type) }}
{% endif %}
-- NICE IND237: https://www.nice.org.uk/indicators/ind237
-- NICE IND238: https://www.nice.org.uk/indicators/ind238
WITH reference_dates AS (
    {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
), assessed AS (
    SELECT population.person_id, dates.reference_date, population.age, population.practice_code,
        profile.bmi_date, profile.bmi_value, profile.is_recorded_white,
        {% if register_type == 'overweight' %}
        IFF(profile.is_recorded_white, 25.0, 23.0)
        {% else %}
        IFF(profile.is_recorded_white, 30.0, 27.5)
        {% endif %} AS bmi_threshold
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN reference_dates AS dates ON population.reporting_date = dates.reference_date
    INNER JOIN {{ nice_ref('int_nice_weight_profile', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    WHERE population.age >= 18
        AND profile.bmi_date > DATEADD(month, -12, dates.reference_date)
        AND profile.bmi_date <= dates.reference_date
        AND profile.bmi_value BETWEEN 5 AND 400
)
SELECT person_id, reference_date, age, practice_code, TRUE AS is_on_register,
    bmi_date AS latest_bmi_date, bmi_value, is_recorded_white, bmi_threshold
FROM assessed
WHERE bmi_value >= bmi_threshold
{% endmacro %}
