{#-
    Calculate NICE IND91 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND91 detail columns, one person per reporting_date.
-#}
{% macro nice_ind91(reference='current') %}
-- NICE IND91: https://www.nice.org.uk/indicators/ind91
-- Current bone-sparing orders or hospital therapy for osteoporosis register members aged 50 to 74.
WITH indicator_population AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('OST', reference) }}) AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
    WHERE population.age BETWEEN 50 AND 74
), assessed AS (
    SELECT population.person_id, population.reporting_date, population.age,
        population.practice_code, population.practice_name,
        therapy.latest_bone_sparing_order_date, therapy.latest_hospital_therapy_date,
        therapy.latest_current_hospital_therapy_date,
        COALESCE(therapy.latest_bone_sparing_order_date > DATEADD(month, -6, population.reporting_date)
            AND therapy.latest_bone_sparing_order_date <= population.reporting_date, FALSE)
            OR therapy.latest_current_hospital_therapy_date IS NOT NULL AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_bone_therapy', reference) }} AS therapy
        ON population.person_id = therapy.person_id
        AND population.reporting_date = therapy.reporting_date
)
SELECT person_id, 'IND91' AS indicator_id,
    'Osteoporosis: bone sparing agents (50-74 years)' AS indicator_name,
    reporting_date, DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age, 'Fragility fracture with osteoporosis, aged 50 to 74' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_bone_sparing_order_date, latest_hospital_therapy_date,
    GREATEST_IGNORE_NULLS(
        IFF(latest_bone_sparing_order_date > DATEADD(month, -6, reporting_date),
            latest_bone_sparing_order_date, NULL),
        latest_current_hospital_therapy_date)::DATE AS latest_record_date,
    TRUE AS is_in_denominator, is_in_numerator,
    CASE WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_bone_sparing_order_date IS NULL AND latest_hospital_therapy_date IS NULL THEN 'NEVER_TREATED'
        ELSE 'NOT_TREATED_IN_PERIOD' END AS indicator_status
FROM assessed
{% endmacro %}
