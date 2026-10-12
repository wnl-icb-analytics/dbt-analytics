{#-
    Calculate NICE IND112 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND112 detail columns, one person per reporting_date.
-#}
{% macro nice_ind112(reference='current') %}
-- NICE IND112: https://www.nice.org.uk/indicators/ind112
-- A complete paired blood pressure record within five years for people aged 40 or over.
WITH population AS (
    SELECT person_id, reporting_date, age, practice_code, practice_name
    FROM ({{ nice_reference_population(reference) }})
    WHERE age >= 40
),

selected AS (
    SELECT population.*, evidence.reading_date AS latest_bp_date
    FROM population
    -- The input already has one complete pair per person and clinical day.
    ASOF JOIN {{ ref('int_nice_blood_pressure_all') }} AS evidence
        MATCH_CONDITION (population.reporting_date >= evidence.reading_date)
        ON population.person_id = evidence.person_id
),

assessed AS (
    SELECT *, COALESCE(latest_bp_date > DATEADD(year, -5, reporting_date), FALSE) AS is_in_numerator
    FROM selected
)

SELECT
    person_id,
    'IND112' AS indicator_id,
    'Cardiovascular disease prevention: blood pressure measurement every 5 years' AS indicator_name,
    'The percentage of patients aged 40 years and over with a blood pressure measurement recorded in the preceding 5 years.' AS indicator_description,
    reporting_date,
    DATEADD(year, -5, reporting_date) AS measurement_period_start,
    age,
    'Aged 40 or over' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_bp_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
