{% macro nice_register(condition_code, reference='current') %}
{% set adapters = {
    'CHD': ('fct_person_chd_register', 'fct_person_chd_register_by_month'),
    'STIA': ('fct_person_stroke_tia_register', 'fct_person_stroke_tia_register_by_month'),
    'PAD': ('fct_person_pad_register', 'fct_person_pad_register_by_month')
} %}
{% if condition_code not in adapters %}
    {{ exceptions.raise_compiler_error('Unsupported NICE register: ' ~ condition_code) }}
{% endif %}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
SELECT person_id,
    {% if reference == 'current' %}CURRENT_DATE()::DATE{% else %}month_end_date{% endif %} AS reporting_date,
    '{{ condition_code }}' AS condition_code, TRUE AS is_on_register,
    earliest_diagnosis_date, latest_diagnosis_date
FROM {{ ref(adapters[condition_code][0 if reference == 'current' else 1]) }}
{% if reference == 'current' %}WHERE is_on_register{% endif %}
{% endmacro %}
