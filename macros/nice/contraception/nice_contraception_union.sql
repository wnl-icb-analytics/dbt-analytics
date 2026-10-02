{% macro nice_contraception_union(reference='current') %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_epilepsy_contraception_advice_ind117' if reference == 'current' else 'fct_person_epilepsy_contraception_advice_ind117_by_month') }}
{% endmacro %}
