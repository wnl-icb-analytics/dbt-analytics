{% macro nice_ind112(reference='current') %}
-- NICE IND112: a complete BP pair in five years, with no clinical exclusions.
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
    reporting_date,
    DATEADD(year, -5, reporting_date) AS measurement_period_start,
    age,
    'Aged 40 and over' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_bp_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
