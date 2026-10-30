{% macro bp_systolic_is_plausible(value) -%}
    ({{ value }} BETWEEN 40 AND 350)
{%- endmacro %}

{% macro bp_diastolic_is_plausible(value) -%}
    ({{ value }} BETWEEN 20 AND 200)
{%- endmacro %}

{% macro bp_pair_is_plausible(systolic, diastolic) -%}
    ({{ bp_systolic_is_plausible(systolic) }}
        AND {{ bp_diastolic_is_plausible(diastolic) }}
        AND {{ systolic }} > {{ diastolic }})
{%- endmacro %}
