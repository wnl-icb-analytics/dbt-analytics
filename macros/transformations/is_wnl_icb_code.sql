{% macro is_wnl_icb_code(code_columns) -%}
{#- True when any supplied organisation code is a WNL ICB, sub-ICB location or legacy borough CCG code;
    null when every input is null. The lookup is read at compile time so the check is a literal list. -#}
{%- set lookup = ref('wnl_commissioner_icb_lookup') -%}
{%- set codes = [] -%}
{%- if execute -%}
    {%- set result = run_query('select distinct upper(trim(commissioner_code)) from ' ~ lookup) -%}
    {%- set codes = result.columns[0].values() -%}
{%- endif -%}
{%- set code_list = "('" ~ (codes | join("', '")) ~ "')" -%}
case
    when {% for code_column in code_columns %}{{ code_column }} is null{% if not loop.last %} and {% endif %}{% endfor %} then null
    else {% for code_column in code_columns %}coalesce(upper(trim({{ code_column }})) in {{ code_list }}, false){% if not loop.last %}
        or {% endif %}{% endfor %}
end
{%- endmacro %}
