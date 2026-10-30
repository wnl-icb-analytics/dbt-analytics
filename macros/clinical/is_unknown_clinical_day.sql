{% macro is_unknown_clinical_day(column) -%}
    ({{ column }} IS NULL
        OR {{ column }}::DATE IN ('1900-01-01'::DATE, '1899-12-31'::DATE))
{%- endmacro %}
