{% macro nice_blood_pressure_register_population(reference='current') %}
{# Active, living, non-test members of the six BP registers, one person/date. #}
WITH bp_registers AS (
    {% for condition in ['HTN', 'CHD', 'STIA', 'PAD', 'CKD', 'DM'] %}
    SELECT person_id, reporting_date
    FROM ({{ nice_register(condition, reference) }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),
bp_people AS (
    SELECT DISTINCT person_id, reporting_date
    FROM bp_registers
)
SELECT people.person_id, people.reporting_date
FROM bp_people AS people
INNER JOIN ({{ nice_reference_population(reference) }}) AS population
    ON people.person_id = population.person_id
    AND people.reporting_date = population.reporting_date
{% endmacro %}
