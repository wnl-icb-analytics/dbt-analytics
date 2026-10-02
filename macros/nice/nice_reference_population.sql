{% macro nice_reference_population(reference='current', scope='active') %}
{#-
    Adapt the population source to the reference date while preserving input scopes.
    Args: reference is current or by_month; scope is active (indicator population),
          cvd (paired register input) or childhood (paired vaccine input).
    Returns: person_id, reporting_date, age, birth_date_approx, gender,
             practice_code, practice_name.
    Current CVD includes inactive register members; current childhood retains the
    programme's registered under-20 population. Monthly inputs use active non-test
    people, with childhood restricted to the established milestone cohorts.
-#}
{% if scope not in ['active', 'cvd', 'childhood'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE population scope: ' ~ scope) }}
{% endif %}
{% if reference == 'current' %}
{% if scope == 'childhood' %}
    SELECT
        population.person_id,
        dates.reporting_date,
        population.age,
        population.birth_date_approx::DATE AS birth_date_approx,
        population.gender,
        population.practice_code,
        population.gp_name AS practice_name
    FROM {{ ref('int_childhood_imms_current_population') }} AS population
    CROSS JOIN ({{ nice_reference_dates(reference) }}) AS dates
{% else %}
    WITH nice_current_person_keys AS (
{% if scope == 'cvd' %}
        {% for condition in ['CHD', 'STIA', 'PAD'] %}
        SELECT person_id
        FROM ({{ nice_register(condition, reference) }})
        {% if not loop.last %}UNION{% endif %}
        {% endfor %}
{% else %}
        SELECT person_id
        FROM {{ ref('dim_person_active_patients') }}
{% endif %}
    )

    SELECT
        nice_current_person_keys.person_id,
        dates.reporting_date,
        age.age,
        age.birth_date_approx,
        demographics.gender,
        active.current_practice_code AS practice_code,
        active.current_practice_name AS practice_name
    FROM nice_current_person_keys
    CROSS JOIN ({{ nice_reference_dates(reference) }}) AS dates
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON nice_current_person_keys.person_id = age.person_id
    LEFT JOIN {{ ref('dim_person_demographics') }} AS demographics
        ON nice_current_person_keys.person_id = demographics.person_id
    LEFT JOIN {{ ref('dim_person_active_patients') }} AS active
        ON nice_current_person_keys.person_id = active.person_id
{% endif %}
{% elif reference == 'by_month' %}
    SELECT
        person_id,
        reporting_date,
        age,
        birth_date_approx,
        gender,
        practice_code,
        practice_name
    FROM {{ ref('int_nice_reference_population_by_month') }}
{% if scope == 'childhood' %}
    WHERE (DATEADD(week, 24, birth_date_approx) > DATEADD(month, -12, reporting_date)
            AND DATEADD(week, 24, birth_date_approx) <= reporting_date)
        OR (DATEADD(month, 8, birth_date_approx) > DATEADD(month, -12, reporting_date)
            AND DATEADD(month, 8, birth_date_approx) <= reporting_date)
        OR (DATEADD(month, 18, birth_date_approx) > DATEADD(month, -12, reporting_date)
            AND DATEADD(month, 18, birth_date_approx) <= reporting_date)
        OR (DATEADD(year, 5, birth_date_approx) > DATEADD(month, -12, reporting_date)
            AND DATEADD(year, 5, birth_date_approx) <= reporting_date)
{% endif %}
{% else %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
{% endmacro %}
