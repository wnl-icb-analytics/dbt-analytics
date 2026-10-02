{% macro nice_reference_population(reference='current') %}
{% if reference == 'current' %}
    SELECT active.person_id, dates.reporting_date, age.age, age.birth_date_approx,
        demographics.gender, active.current_practice_code AS practice_code,
        active.current_practice_name AS practice_name
    FROM {{ ref('dim_person_active_patients') }} active
    CROSS JOIN ({{ nice_reference_dates(reference) }}) dates
    LEFT JOIN {{ ref('dim_person_age') }} age ON active.person_id = age.person_id
    LEFT JOIN {{ ref('dim_person_demographics') }} demographics ON active.person_id = demographics.person_id
{% elif reference == 'by_month' %}
    SELECT person_id, reporting_date, age, birth_date_approx, gender, practice_code, practice_name
    FROM {{ ref('int_nice_reference_population_by_month') }}
{% else %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
{% endmacro %}
