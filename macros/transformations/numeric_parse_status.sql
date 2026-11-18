{% macro numeric_parse_status(value) -%}
    case
        when {{ value }} is null then 'value_missing'
        when try_to_decimal({{ value }}, 38, 9) is null then 'not_numeric_or_out_of_range'
        when not regexp_like(trim({{ value }}), '[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)')
            then 'numeric_format_unverified'
        when not regexp_like(trim({{ value }}), '[+-]?([0-9]+([.][0-9]{0,9}0*)?|[.][0-9]{1,9}0*)')
            then 'numeric_rounded'
        else 'numeric'
    end
{%- endmacro %}
