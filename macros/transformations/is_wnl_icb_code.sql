{% macro is_wnl_icb_code(code_columns) -%}
{#- True when any supplied organisation code is a WNL ICB, sub-ICB location or legacy borough CCG code;
    false when codes are present but none match; null when every input is null or blank. The lookup is
    read in the query as a one-row array, so compilation never needs the seed to exist. -#}
{%- set wnl_codes -%}(select array_agg(distinct upper(trim(commissioner_code))) from {{ ref('wnl_commissioner_icb_lookup') }}){%- endset -%}
case
    when {% for code_column in code_columns %}nullif(trim({{ code_column }}), '') is null{% if not loop.last %} and {% endif %}{% endfor %} then null
    else {% for code_column in code_columns %}coalesce(array_contains(nullif(upper(trim({{ code_column }})), '')::variant, {{ wnl_codes }}), false){% if not loop.last %}
        or {% endif %}{% endfor %}
end
{%- endmacro %}
