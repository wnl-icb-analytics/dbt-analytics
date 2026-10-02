{% macro calculate_nice_bone_therapy(reference='current') %}
WITH population AS (
    SELECT person_id, reporting_date
    FROM ({{ nice_reference_population(reference) }})
    WHERE age >= 50
), daily AS (
    SELECT person_id, order_date::DATE AS order_date
    FROM {{ ref('int_bone_sparing_medications_all') }}
    GROUP BY person_id, order_date::DATE
)
SELECT population.person_id, population.reporting_date,
    daily.order_date AS latest_bone_sparing_order_date
FROM population
ASOF JOIN daily
    MATCH_CONDITION (population.reporting_date >= daily.order_date)
    ON population.person_id = daily.person_id
{% endmacro %}
