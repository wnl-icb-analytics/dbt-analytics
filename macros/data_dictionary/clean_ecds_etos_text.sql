{#
    Trims ETOS text and nulls blanks. ETOS overwrites most fields of a
    deprecated code with the literal 'Code deprecated'; staging records that
    as is_deprecated and nulls the marker.
#}
{% macro clean_ecds_etos_text(column) %}
nullif(nullif(trim({{ column }}), ''), 'Code deprecated')
{%- endmacro %}

{# ETOS flags hold '1' or '0'; 'N/A', 'Code deprecated' and blanks become null. #}
{% macro ecds_etos_flag(column) %}
case trim({{ column }}) when '1' then true when '0' then false end
{%- endmacro %}
