{% macro is_wnl_icb_code(code_columns) -%}
{#- True when any supplied organisation code is a WNL ICB, sub-ICB location or legacy borough CCG code. -#}
coalesce(
{%- for code_column in code_columns %}
    upper(trim({{ code_column }})) in (select upper(commissioner_code) from {{ ref('wnl_commissioner_icb_lookup') }})
    {%- if not loop.last %} or{% endif %}
{%- endfor %}
    , false
)
{%- endmacro %}
