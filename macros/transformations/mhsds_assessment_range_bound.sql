{% macro mhsds_assessment_range_bound(value, bound) -%}
    {% if bound not in ['minimum', 'maximum'] %}
        {{ exceptions.raise_compiler_error('Assessment range bound must be minimum or maximum') }}
    {% endif %}
    try_to_decimal(regexp_substr(
        {{ value }}
        , '^(-?[0-9]+([.][0-9]+)?) *- *(-?[0-9]+([.][0-9]+)?)$'
        , 1, 1, 'e', {{ 1 if bound == 'minimum' else 3 }}
    ), 38, 9)
{%- endmacro %}
