{%- macro encode_ltc_lcs_risk_group(risk_group_field) -%}
case
    when {{ risk_group_field }} is null then 0
    when upper(trim({{ risk_group_field }})) = 'HRC' then 4
    when upper(trim({{ risk_group_field }})) = 'HR' then 3
    when upper(trim({{ risk_group_field }})) like 'MR%' then 2
    when upper(trim({{ risk_group_field }})) = 'LR' then 1
    else 0
end
{%- endmacro %}
