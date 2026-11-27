{% macro bp_systolic_is_plausible(value) -%}
    {# SQL expression in mmHg; inclusive 40 to 350. NULL input returns NULL. #}
    ({{ value }} BETWEEN 40 AND 350)
{%- endmacro %}

{% macro bp_diastolic_is_plausible(value) -%}
    {# SQL expression in mmHg; inclusive 20 to 200. NULL input returns NULL. #}
    ({{ value }} BETWEEN 20 AND 200)
{%- endmacro %}

{% macro bp_pair_is_plausible(systolic, diastolic) -%}
    {# SQL expressions in mmHg, expanded twice each; use deterministic expressions.
       Both inclusive ranges must pass and systolic must exceed diastolic.
       NULL can propagate unless another predicate is FALSE. #}
    ({{ bp_systolic_is_plausible(systolic) }}
        AND {{ bp_diastolic_is_plausible(diastolic) }}
        AND {{ systolic }} > {{ diastolic }})
{%- endmacro %}
