{% macro numeric_parse_status(value) -%}
    case
        when {{ value }} is null then 'no value'
        when try_to_decimal({{ value }}, 38, 9) is null then 'not numeric or out of range'
        when not regexp_like(trim({{ value }}), '[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)')
            then 'numeric (format not checked)'
        when not regexp_like(trim({{ value }}), '[+-]?([0-9]+([.][0-9]{0,9}0*)?|[.][0-9]{1,9}0*)')
            then 'numeric (rounded)'
        else 'numeric'
    end
{%- endmacro %}
